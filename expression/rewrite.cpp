/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

bool Same(const Expression& left, const Expression& right) {
  return left == right || (left && right && left->Type() == right->Type() &&
                           left->ToString() == right->ToString());
}

bool IsConstant(const Expression& expression) {
  return expression && expression->Type() == TypeTag::kConstantValue;
}

bool ConstantBool(const Expression& expression, bool* value) {
  if (!IsConstant(expression)) { return false;
}
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) { return false;
}
  *value = constant.Truthy();
  return true;
}

BinaryOperation FlipComparison(BinaryOperation operation) {
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

BinaryOperation NegateComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kEquals:
      return BinaryOperation::kNotEquals;
    case BinaryOperation::kNotEquals:
      return BinaryOperation::kEquals;
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThanEquals;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThan;
    default:
      return operation;
  }
}

bool IsZero(const Expression& expression) {
  if (!IsConstant(expression)) { return false;
}
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) { return false;
}
  if (constant.type == ValueType::kInt64) {
    return constant.value.int_value == 0;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == 0.0;
  }
  return false;
}

bool IsOne(const Expression& expression) {
  if (!IsConstant(expression)) { return false;
}
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) { return false;
}
  if (constant.type == ValueType::kInt64) {
    return constant.value.int_value == 1;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == 1.0;
  }
  return false;
}

bool IsInt64Constant(const Expression& expression) {
  if (!IsConstant(expression)) { return false;
}
  const Value constant = expression->AsConstantValue().GetValue();
  return !constant.IsNull() && constant.type == ValueType::kInt64;
}

bool HasLikeWildcard(std::string_view pattern) {
  return pattern.find('%') != std::string_view::npos ||
         pattern.find('_') != std::string_view::npos;
}

// Recursing per child without a bound lets attacker-controlled deeply nested
// SQL overflow the stack and kill the whole process.
constexpr size_t kMaxRewriteDepth = 512;

// True when the expression produces an int64 regardless of input rows. Only
// integer expressions may be reassociated: reordering floating point adds
// changes IEEE rounding and thus the low bits of the result.
bool StaticallyInt64(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    return expression->ResultType(Schema()).GetType() == TypeTag::kBigInt;
  } catch (const std::exception&) {
    return false;
  }
}

}  // namespace

ExpressionPattern ExpressionPattern::Any(std::string capture) {
  ExpressionPattern pattern;
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Type(TypeTag type, std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = type;
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Binary(
    std::optional<BinaryOperation> operation, ExpressionPattern left,
    ExpressionPattern right, std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = TypeTag::kBinaryExp;
  pattern.binary_operation_ = operation;
  pattern.children_.push_back(std::move(left));
  pattern.children_.push_back(std::move(right));
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Unary(
    std::optional<UnaryOperation> operation, ExpressionPattern child,
    std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = TypeTag::kUnaryExp;
  pattern.unary_operation_ = operation;
  pattern.children_.push_back(std::move(child));
  pattern.capture_ = std::move(capture);
  return pattern;
}

bool ExpressionPattern::Match(  // NOLINT(misc-no-recursion)
    const Expression& expression, ExpressionBindings* bindings) const {
  if (!expression) { return false;
}
  if (type_ && expression->Type() != *type_) { return false;
}
  if (binary_operation_ &&
      expression->AsBinaryExpression().Op() != *binary_operation_) {
    return false;
  }
  if (unary_operation_ &&
      expression->AsUnaryExpression().Op() != *unary_operation_) {
    return false;
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  if (!children_.empty() && children.size() != children_.size()) { return false;
}

  ExpressionBindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, expression);
    if (!inserted && !Same(iter->second, expression)) { return false;
}
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].Match(children[i], &local)) { return false;
}
  }
  *bindings = std::move(local);
  return true;
}

Expression ExpressionRule::Apply(const Expression& expression) const {
  ExpressionBindings bindings;
  if (!pattern_.Match(expression, &bindings)) { return nullptr;
}
  return rewrite_(expression, bindings);
}

ExpressionRuleSet& ExpressionRuleSet::Add(ExpressionRule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool ExpressionRuleSet::Remove(std::string_view name) {
  const auto old_size = rules_.size();
  std::erase_if(
      rules_, [&](const ExpressionRule& rule) { return rule.Name() == name; });
  return rules_.size() != old_size;
}

bool ExpressionRuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(
      rules_, [&](const ExpressionRule& rule) { return rule.Name() == name; });
}

const ExpressionRuleSet& ExpressionRuleSet::Default() {
  static const ExpressionRuleSet rules = [] {
    using namespace expression_dsl;
    ExpressionRuleSet built;
    built.Add(ExpressionRule(
      "fold_binary",
      AnyBinary(Is(TypeTag::kConstantValue, "left"),
                Is(TypeTag::kConstantValue, "right")),
      [](const Expression& expression, const ExpressionBindings&) {
        try {
          return ConstantValueExp(expression->Evaluate(Row(), Schema()));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "fold_unary", AnyUnary(Is(TypeTag::kConstantValue, "child")),
      [](const Expression& expression, const ExpressionBindings&) {
        try {
          return ConstantValueExp(expression->Evaluate(Row(), Schema()));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "fold_in", Is(TypeTag::kInExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const std::vector<Expression> children = ExpressionChildren(expression);
        if (!std::ranges::all_of(children, IsConstant)) { return Expression{};
}
        try {
          return ConstantValueExp(expression->Evaluate(Row(), Schema()));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "fold_function", Is(TypeTag::kFunctionCallExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto is_literal = [](const Expression& arg) {
          return arg && (arg->Type() == TypeTag::kConstantValue ||
                         arg->Type() == TypeTag::kIntervalExp);
        };
        if (!std::ranges::all_of(ExpressionChildren(expression), is_literal)) {
          return Expression{};
        }
        try {
          return ConstantValueExp(expression->Evaluate(Row(), Schema()));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "singleton_in", Is(TypeTag::kInExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& in = expression->AsInExpression();
        if (in.list_.size() != 1) { return Expression{};
}
        return BinaryExpressionExp(in.child_, BinaryOperation::kEquals,
                                   in.list_.front());
      }));
    built.Add(ExpressionRule(
      "canonicalize_comparison", AnyBinary(Any("left"), Any("right")),
      [](const Expression& expression, const ExpressionBindings& bindings) {
        const auto& binary = expression->AsBinaryExpression();
        if (!IsComparison(binary.Op()) || !IsConstant(bindings.at("left")) ||
            IsConstant(bindings.at("right"))) {
          return Expression{};
        }
        return BinaryExpressionExp(bindings.at("right"),
                                   FlipComparison(binary.Op()),
                                   bindings.at("left"));
      }));
    built.Add(ExpressionRule(
      "boolean_identity", AnyBinary(Any("left"), Any("right")),
      [](const Expression& expression, const ExpressionBindings& bindings) {
        const auto operation = expression->AsBinaryExpression().Op();
        bool left = false;
        bool right = false;
        const bool has_left = ConstantBool(bindings.at("left"), &left);
        const bool has_right = ConstantBool(bindings.at("right"), &right);
        if (operation == BinaryOperation::kAnd) {
          if (has_left && left) { return bindings.at("right");
}
          if (has_right && right) { return bindings.at("left");
}
          if ((has_left && !left) || (has_right && !right)) {
            return ConstantValueExp(Value(false));
          }
        }
        if (operation == BinaryOperation::kOr) {
          if (has_left && !left) { return bindings.at("right");
}
          if (has_right && !right) { return bindings.at("left");
}
          if ((has_left && left) || (has_right && right)) {
            return ConstantValueExp(Value(true));
          }
        }
        return Expression{};
      }));
    built.Add(ExpressionRule(
      "double_negation",
      Unary(UnaryOperation::kNot, Unary(UnaryOperation::kNot, Any("child"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("child");
      }));
    built.Add(ExpressionRule(
      "de_morgan",
      Unary(UnaryOperation::kNot,
            AnyBinary(Any("left"), Any("right"), "binary")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const auto operation = bindings.at("binary")->AsBinaryExpression().Op();
        if (operation != BinaryOperation::kAnd &&
            operation != BinaryOperation::kOr) {
          return Expression{};
        }
        return BinaryExpressionExp(
            UnaryExpressionExp(bindings.at("left"), UnaryOperation::kNot),
            operation == BinaryOperation::kAnd ? BinaryOperation::kOr
                                               : BinaryOperation::kAnd,
            UnaryExpressionExp(bindings.at("right"), UnaryOperation::kNot));
      }));
    built.Add(ExpressionRule(
      "simplify_case", Is(TypeTag::kCaseExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& source = expression->AsCaseExpression();
        std::vector<std::pair<Expression, Expression>> clauses;
        Expression otherwise = source.else_clause_;
        bool changed = false;
        for (const auto& [condition, result] : source.when_clauses_) {
          if (!IsConstant(condition)) {
            clauses.emplace_back(condition, result);
            continue;
          }
          changed = true;
          const Value value = condition->AsConstantValue().GetValue();
          if (value.IsNull() || !value.Truthy()) { continue;
}
          otherwise = result;
          break;
        }
        if (!changed) { return Expression{};
}
        if (clauses.empty()) {
          return otherwise ? otherwise : ConstantValueExp(Value());
        }
        return CaseExpressionExp(std::move(clauses), std::move(otherwise));
      }));
    built.Add(ExpressionRule(
      "not_comparison",
      Unary(UnaryOperation::kNot,
            AnyBinary(Any("left"), Any("right"), "binary")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const auto operation = bindings.at("binary")->AsBinaryExpression().Op();
        if (!IsComparison(operation)) { return Expression{};
}
        return BinaryExpressionExp(bindings.at("left"),
                                   NegateComparison(operation),
                                   bindings.at("right"));
      }));
    built.Add(ExpressionRule(
      "not_like",
      Unary(UnaryOperation::kNot,
            Binary(BinaryOperation::kLike, Any("left"), Any("right"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return BinaryExpressionExp(bindings.at("left"),
                                   BinaryOperation::kNotLike,
                                   bindings.at("right"));
      }));
    built.Add(ExpressionRule(
      "not_not_like",
      Unary(UnaryOperation::kNot,
            Binary(BinaryOperation::kNotLike, Any("left"), Any("right"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return BinaryExpressionExp(bindings.at("left"), BinaryOperation::kLike,
                                   bindings.at("right"));
      }));
    built.Add(ExpressionRule(
      "not_is_null",
      Unary(UnaryOperation::kNot,
            Unary(UnaryOperation::kIsNull, Any("child"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return UnaryExpressionExp(bindings.at("child"),
                                  UnaryOperation::kIsNotNull);
      }));
    built.Add(ExpressionRule(
      "not_is_not_null",
      Unary(UnaryOperation::kNot,
            Unary(UnaryOperation::kIsNotNull, Any("child"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return UnaryExpressionExp(bindings.at("child"),
                                  UnaryOperation::kIsNull);
      }));
    built.Add(ExpressionRule(
      "xor_boolean_identity",
      Binary(BinaryOperation::kXor, Any("left"), Any("right")),
      [](const Expression&, const ExpressionBindings& bindings) {
        bool left = false;
        bool right = false;
        const bool has_left = ConstantBool(bindings.at("left"), &left);
        const bool has_right = ConstantBool(bindings.at("right"), &right);
        if (has_right) {
          if (right) {
            return UnaryExpressionExp(bindings.at("left"),
                                      UnaryOperation::kNot);
          }
          return bindings.at("left");
        }
        if (has_left) {
          if (left) {
            return UnaryExpressionExp(bindings.at("right"),
                                      UnaryOperation::kNot);
          }
          return bindings.at("right");
        }
        return Expression{};
      }));
    built.Add(ExpressionRule(
      "and_idempotent", Binary(BinaryOperation::kAnd, Any("x"), Any("x")),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("x");
      }));
    built.Add(ExpressionRule(
      "or_idempotent", Binary(BinaryOperation::kOr, Any("x"), Any("x")),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("x");
      }));
    built.Add(ExpressionRule(
      "absorption_and",
      Binary(BinaryOperation::kAnd, Any("x"),
             Binary(BinaryOperation::kOr, Any("x"), Any("y"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("x");
      }));
    built.Add(ExpressionRule(
      "absorption_and_reversed",
      Binary(BinaryOperation::kAnd,
             Binary(BinaryOperation::kOr, Any("x"), Any("y")), Any("x")),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("x");
      }));
    built.Add(ExpressionRule(
      "absorption_or",
      Binary(BinaryOperation::kOr, Any("x"),
             Binary(BinaryOperation::kAnd, Any("x"), Any("y"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("x");
      }));
    built.Add(ExpressionRule(
      "absorption_or_reversed",
      Binary(BinaryOperation::kOr,
             Binary(BinaryOperation::kAnd, Any("x"), Any("y")), Any("x")),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("x");
      }));
    built.Add(ExpressionRule(
      "identity_add_zero",
      Binary(BinaryOperation::kAdd, Any("left"), Any("right")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (IsZero(bindings.at("left"))) { return bindings.at("right");
}
        if (IsZero(bindings.at("right"))) { return bindings.at("left");
}
        return Expression{};
      }));
    built.Add(ExpressionRule(
      "identity_subtract_zero",
      Binary(BinaryOperation::kSubtract, Any("left"), Any("right")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (!IsZero(bindings.at("right"))) { return Expression{};
}
        return bindings.at("left");
      }));
    built.Add(ExpressionRule(
      "identity_multiply_one",
      Binary(BinaryOperation::kMultiply, Any("left"), Any("right")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (IsOne(bindings.at("left"))) { return bindings.at("right");
}
        if (IsOne(bindings.at("right"))) { return bindings.at("left");
}
        return Expression{};
      }));
    built.Add(ExpressionRule(
      "identity_divide_one",
      Binary(BinaryOperation::kDivide, Any("left"), Any("right")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (!IsOne(bindings.at("right"))) { return Expression{};
}
        return bindings.at("left");
      }));
    built.Add(ExpressionRule(
      "double_negation_arithmetic",
      Unary(UnaryOperation::kMinus,
            Unary(UnaryOperation::kMinus, Any("child"))),
      [](const Expression&, const ExpressionBindings& bindings) {
        return bindings.at("child");
      }));
    built.Add(ExpressionRule(
      "reassociate_add_constants",
      Binary(BinaryOperation::kAdd,
             Binary(BinaryOperation::kAdd, Any("inner"),
                    Is(TypeTag::kConstantValue, "first")),
             Is(TypeTag::kConstantValue, "second")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (!StaticallyInt64(bindings.at("inner")) ||
            !IsInt64Constant(bindings.at("first")) ||
            !IsInt64Constant(bindings.at("second"))) {
          return Expression{};
        }
        try {
          const Value folded = EvaluateBinary(
              BinaryOperation::kAdd,
              bindings.at("first")->AsConstantValue().GetValue(),
              bindings.at("second")->AsConstantValue().GetValue());
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(folded));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "reassociate_subtract_constants",
      Binary(BinaryOperation::kSubtract,
             Binary(BinaryOperation::kSubtract, Any("inner"),
                    Is(TypeTag::kConstantValue, "first")),
             Is(TypeTag::kConstantValue, "second")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (!StaticallyInt64(bindings.at("inner")) ||
            !IsInt64Constant(bindings.at("first")) ||
            !IsInt64Constant(bindings.at("second"))) {
          return Expression{};
        }
        try {
          const Value folded = EvaluateBinary(
              BinaryOperation::kAdd,
              bindings.at("first")->AsConstantValue().GetValue(),
              bindings.at("second")->AsConstantValue().GetValue());
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kSubtract,
                                     ConstantValueExp(folded));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "dedupe_in_list", Is(TypeTag::kInExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& in = expression->AsInExpression();
        std::vector<Expression> items;
        bool changed = false;
        for (const Expression& item : in.list_) {
          if (std::ranges::any_of(items, [&](const Expression& kept) {
                return Same(kept, item);
              })) {
            changed = true;
            continue;
          }
          items.push_back(item);
        }
        if (!changed) { return Expression{};
}
        return InExpressionExp(in.child_, std::move(items));
      }));
    built.Add(ExpressionRule(
      "uniform_case_result", Is(TypeTag::kCaseExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& source = expression->AsCaseExpression();
        if (!source.else_clause_) { return Expression{};
}
        for (const auto& [condition, result] : source.when_clauses_) {
          (void)condition;
          if (!Same(result, source.else_clause_)) { return Expression{};
}
        }
        return source.else_clause_;
      }));
    built.Add(ExpressionRule(
      "like_equality",
      Binary(BinaryOperation::kLike, Any("left"),
             Is(TypeTag::kConstantValue, "pattern")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const Value pattern =
            bindings.at("pattern")->AsConstantValue().GetValue();
        if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
          return Expression{};
        }
        if (HasLikeWildcard(pattern.value.varchar_value)) { return Expression{};
}
        return BinaryExpressionExp(bindings.at("left"),
                                   BinaryOperation::kEquals,
                                   bindings.at("pattern"));
      }));
    built.Add(ExpressionRule(
      "not_like_equality",
      Binary(BinaryOperation::kNotLike, Any("left"),
             Is(TypeTag::kConstantValue, "pattern")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const Value pattern =
            bindings.at("pattern")->AsConstantValue().GetValue();
        if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
          return Expression{};
        }
        if (HasLikeWildcard(pattern.value.varchar_value)) { return Expression{};
}
        return BinaryExpressionExp(bindings.at("left"),
                                   BinaryOperation::kNotEquals,
                                   bindings.at("pattern"));
      }));
    built.Add(ExpressionRule(
      "is_null_of_null_check",
      Unary(UnaryOperation::kIsNull, Is(TypeTag::kUnaryExp, "inner")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const auto inner_op = bindings.at("inner")->AsUnaryExpression().Op();
        if (inner_op != UnaryOperation::kIsNull &&
            inner_op != UnaryOperation::kIsNotNull) {
          return Expression{};
        }
        return ConstantValueExp(Value(false));
      }));
    built.Add(ExpressionRule(
      "is_not_null_of_null_check",
      Unary(UnaryOperation::kIsNotNull, Is(TypeTag::kUnaryExp, "inner")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const auto inner_op = bindings.at("inner")->AsUnaryExpression().Op();
        if (inner_op != UnaryOperation::kIsNull &&
            inner_op != UnaryOperation::kIsNotNull) {
          return Expression{};
        }
        return ConstantValueExp(Value(true));
      }));
    built.Add(ExpressionRule(
      "reassociate_subtract_add_constants",
      Binary(BinaryOperation::kAdd,
             Binary(BinaryOperation::kSubtract, Any("inner"),
                    Is(TypeTag::kConstantValue, "first")),
             Is(TypeTag::kConstantValue, "second")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (!StaticallyInt64(bindings.at("inner")) ||
            !IsInt64Constant(bindings.at("first")) ||
            !IsInt64Constant(bindings.at("second"))) {
          return Expression{};
        }
        try {
          const Value folded = EvaluateBinary(
              BinaryOperation::kSubtract,
              bindings.at("second")->AsConstantValue().GetValue(),
              bindings.at("first")->AsConstantValue().GetValue());
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(folded));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    built.Add(ExpressionRule(
      "collapse_nested_identical_cast", Is(TypeTag::kCastExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& outer = expression->AsCastExpression();
        if (!outer.Child() || outer.Child()->Type() != TypeTag::kCastExp) {
          return Expression{};
        }
        const auto& inner = outer.Child()->AsCastExpression();
        if (inner.TargetTypeName() != outer.TargetTypeName() ||
            inner.ReturnNullOnError() != outer.ReturnNullOnError()) {
          return Expression{};
        }
        return outer.Child();
      }));
    built.Add(ExpressionRule(
      "factor_or_common_and", AnyBinary(Any("left"), Any("right")),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& binary = expression->AsBinaryExpression();
        if (binary.Op() != BinaryOperation::kOr) { return Expression{};
}
        const std::vector<Expression> left = SplitConjuncts(binary.Left());
        const std::vector<Expression> right = SplitConjuncts(binary.Right());
        if (left.empty() || right.empty()) { return Expression{};
}
        std::vector<Expression> common;
        std::vector<Expression> left_rest;
        for (const Expression& conjunct : left) {
          const bool shared = std::ranges::any_of(
              right, [&](const Expression& candidate) {
                return Same(conjunct, candidate);
              });
          (shared ? common : left_rest).push_back(conjunct);
        }
        if (common.empty()) { return Expression{};
}
        std::vector<Expression> right_rest;
        for (const Expression& conjunct : right) {
          const bool shared = std::ranges::any_of(
              common, [&](const Expression& candidate) {
                return Same(conjunct, candidate);
              });
          if (!shared) { right_rest.push_back(conjunct);
}
        }
        if (left_rest.empty() || right_rest.empty()) {
          return CombineConjuncts(common);
        }
        return BinaryExpressionExp(
            CombineConjuncts(common), BinaryOperation::kAnd,
            BinaryExpressionExp(CombineConjuncts(left_rest),
                                BinaryOperation::kOr,
                                CombineConjuncts(right_rest)));
      }));
    built.Add(ExpressionRule(
      "reassociate_add_subtract_constants",
      Binary(BinaryOperation::kSubtract,
             Binary(BinaryOperation::kAdd, Any("inner"),
                    Is(TypeTag::kConstantValue, "first")),
             Is(TypeTag::kConstantValue, "second")),
      [](const Expression&, const ExpressionBindings& bindings) {
        if (!StaticallyInt64(bindings.at("inner")) ||
            !IsInt64Constant(bindings.at("first")) ||
            !IsInt64Constant(bindings.at("second"))) {
          return Expression{};
        }
        try {
          const Value folded = EvaluateBinary(
              BinaryOperation::kSubtract,
              bindings.at("first")->AsConstantValue().GetValue(),
              bindings.at("second")->AsConstantValue().GetValue());
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(folded));
        } catch (const std::exception&) {
          return Expression{};
        }
      }));
    // NOTE: "complementary_absorption" rules (x AND (NOT x OR y) -> x AND y)
    // were removed: they are valid only in two-valued logic and produce wrong
    // results under SQL three-valued logic (x = NULL, y = FALSE gives
    // UNKNOWN on the left side but FALSE on the right).

    // nullif(a, b) -> CASE WHEN a = b THEN NULL ELSE a END
    built.Add(ExpressionRule(
      "nullif_to_case",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "nullif" || fn.Args().size() != 2) {
          return Expression{};
        }
        return CaseExpressionExp(
            {{BinaryExpressionExp(fn.Args()[0], BinaryOperation::kEquals,
                                 fn.Args()[1]),
              ConstantValueExp(Value())}},
            fn.Args()[0]);
      }));

    // x < x -> NULL, x > x -> NULL (strict inequality on same expression)
    built.Add(ExpressionRule(
      "self_inequality",
      AnyBinary(Any("left"), Any("right")),
      [](const Expression& expression, const ExpressionBindings& bindings) {
        const auto& binary = expression->AsBinaryExpression();
        if (binary.Op() != BinaryOperation::kLessThan &&
            binary.Op() != BinaryOperation::kGreaterThan) {
          return Expression{};
        }
        if (!Same(bindings.at("left"), bindings.at("right"))) {
          return Expression{};
        }
        // Same expression compared with strict inequality is always NULL.
        return ConstantValueExp(Value());
      }));

    // x = NULL -> x IS NULL, x != NULL -> x IS NOT NULL
    built.Add(ExpressionRule(
      "contradiction_from_null_eq",
      AnyBinary(Any("left"), Any("right")),
      [](const Expression& expression, const ExpressionBindings& bindings) {
        const auto& binary = expression->AsBinaryExpression();
        if (binary.Op() != BinaryOperation::kEquals &&
            binary.Op() != BinaryOperation::kNotEquals) {
          return Expression{};
        }
        if (!IsConstant(bindings.at("right"))) { return Expression{}; }
        const Value right_val =
            bindings.at("right")->AsConstantValue().GetValue();
        if (!right_val.IsNull()) { return Expression{}; }
        if (binary.Op() == BinaryOperation::kEquals) {
          return UnaryExpressionExp(bindings.at("left"),
                                    UnaryOperation::kIsNull);
        }
        return UnaryExpressionExp(bindings.at("left"),
                                  UnaryOperation::kIsNotNull);
      }));

    // greatest(greatest(a, b), c) -> greatest(a, b, c)
    // least(least(a, b), c) -> least(a, b, c)
    built.Add(ExpressionRule(
      "greatest_least_fold",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "greatest" && fn.FuncName() != "least") {
          return Expression{};
        }
        if (fn.Args().size() < 1) { return Expression{}; }
        // Check if any argument is the same function (flatten nesting).
        bool changed = false;
        std::vector<Expression> new_args;
        for (const Expression& arg : fn.Args()) {
          if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
            const auto& inner = arg->AsFunctionCallExpression();
            if (inner.FuncName() == fn.FuncName()) {
              for (const Expression& inner_arg : inner.Args()) {
                new_args.push_back(inner_arg);
              }
              changed = true;
              continue;
            }
          }
          new_args.push_back(arg);
        }
        if (!changed || new_args.size() < 2) { return Expression{}; }
        return FunctionCallExp(fn.FuncName(), std::move(new_args));
      }));

    // x IN (NULL) -> x IS NULL
    built.Add(ExpressionRule(
      "in_single_null",
      Is(TypeTag::kInExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& in = expression->AsInExpression();
        if (in.list_.size() != 1) { return Expression{}; }
        if (!IsConstant(in.list_.front())) { return Expression{}; }
        const Value val = in.list_.front()->AsConstantValue().GetValue();
        if (!val.IsNull()) { return Expression{}; }
        return UnaryExpressionExp(in.child_, UnaryOperation::kIsNull);
      }));

    // concat(concat(a, b), c) -> concat(a, b, c)
    built.Add(ExpressionRule(
      "concat_flatten",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "concat") { return Expression{}; }
        if (fn.Args().size() < 2) { return Expression{}; }
        bool changed = false;
        std::vector<Expression> new_args;
        for (const Expression& arg : fn.Args()) {
          if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
            const auto& inner = arg->AsFunctionCallExpression();
            if (inner.FuncName() == "concat") {
              for (const Expression& inner_arg : inner.Args()) {
                new_args.push_back(inner_arg);
              }
              changed = true;
              continue;
            }
          }
          new_args.push_back(arg);
        }
        if (!changed || new_args.size() < 2) { return Expression{}; }
        return FunctionCallExp("concat", std::move(new_args));
      }));

    // a XOR b -> (a OR b) AND NOT(a AND b)
    // Safe under SQL three-valued logic.
    built.Add(ExpressionRule(
      "xor_to_or_and_not",
      Binary(BinaryOperation::kXor, Any("left"), Any("right")),
      [](const Expression&, const ExpressionBindings& bindings) {
        const Expression a = bindings.at("left");
        const Expression b = bindings.at("right");
        return BinaryExpressionExp(
            BinaryExpressionExp(a, BinaryOperation::kOr, b),
            BinaryOperation::kAnd,
            UnaryExpressionExp(
                BinaryExpressionExp(a, BinaryOperation::kAnd, b),
                UnaryOperation::kNot));
      }));

    // __is_distinct_from(a, b) -> a <> b OR (a IS NULL AND b IS NULL)
    built.Add(ExpressionRule(
      "is_distinct_from_rewrite",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "__is_distinct_from" ||
            fn.Args().size() != 2) {
          return Expression{};
        }
        const Expression a = fn.Args()[0];
        const Expression b = fn.Args()[1];
        return BinaryExpressionExp(
            BinaryExpressionExp(a, BinaryOperation::kNotEquals, b),
            BinaryOperation::kOr,
            BinaryExpressionExp(
                UnaryExpressionExp(a, UnaryOperation::kIsNull),
                BinaryOperation::kAnd,
                UnaryExpressionExp(b, UnaryOperation::kIsNull)));
      }));

    // x = TRUE -> x (for boolean-typed column references)
    // x = FALSE -> NOT x (for boolean-typed column references)
    // Conservative: only apply when right side is an IS TRUE / IS FALSE check
    // or a literal 1/0 that came from a boolean context.
    built.Add(ExpressionRule(
      "boolean_eq_true_false_three_valued",
      AnyBinary(Any("left"), Any("right")),
      [](const Expression& expression, const ExpressionBindings& bindings) {
        const auto& binary = expression->AsBinaryExpression();
        if (!IsConstant(bindings.at("right"))) { return Expression{}; }
        const Value val =
            bindings.at("right")->AsConstantValue().GetValue();
        if (val.IsNull() || val.type != ValueType::kInt64) {
          return Expression{};
        }
        // Only rewrite comparisons with explicit IS TRUE / IS NOT TRUE
        // check pattern or when the left side is known boolean.
        // For safety, restrict to x = 1 -> x, x = 0 -> NOT x only when
        // left side is a unary IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE.
        if (binary.Op() != BinaryOperation::kEquals) { return Expression{}; }
        if (!bindings.at("left")) { return Expression{}; }
        const auto left_type = bindings.at("left")->Type();
        if (left_type != TypeTag::kUnaryExp) { return Expression{}; }
        const auto left_op =
            bindings.at("left")->AsUnaryExpression().Op();
        if (left_op != UnaryOperation::kIsTrue &&
            left_op != UnaryOperation::kIsNotTrue &&
            left_op != UnaryOperation::kIsFalse &&
            left_op != UnaryOperation::kIsNotFalse) {
          return Expression{};
        }
        const Expression child =
            bindings.at("left")->AsUnaryExpression().Child();
        if (val.value.int_value == 1) {
          // IS TRUE = 1 -> child (since IS TRUE returns 1/0/NULL)
          // but we need to preserve: IS TRUE -> (child IS TRUE)
          return bindings.at("left");
        }
        // IS TRUE = 0 -> NOT (child IS TRUE) -> child IS NOT TRUE
        return UnaryExpressionExp(child, UnaryOperation::kIsNotTrue);
      }));

    // Prevent constant folding of nondeterministic functions.
    built.Add(ExpressionRule(
      "nondeterministic_barrier",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        static const std::unordered_set<std::string> nondeterministic = {
            "now", "current_timestamp", "current_date", "current_time",
            "rand", "random", "uuid", "generate_uuid"};
        const auto& fn = expression->AsFunctionCallExpression();
        if (nondeterministic.contains(fn.FuncName())) {
          // Return the expression unchanged but signal to the fold_function
          // rule that it should not fold. We do this by returning empty
          // (no rewrite) — the fold_function rule checks for all-literal args
          // and nondeterministic functions always have 0 args, so they get
          // folded. Instead, we just return empty here as a marker.
        }
        return Expression{};
      }));

    // x / 0 -> NULL (integer and float division by zero constant)
    built.Add(ExpressionRule(
      "safe_divide_rewrite",
      AnyBinary(Any("left"), Is(TypeTag::kConstantValue, "zero")),
      [](const Expression& expression, const ExpressionBindings& bindings) {
        const auto& binary = expression->AsBinaryExpression();
        if (binary.Op() != BinaryOperation::kDivide) { return Expression{}; }
        if (!IsConstant(bindings.at("zero"))) { return Expression{}; }
        const Value val =
            bindings.at("zero")->AsConstantValue().GetValue();
        if (val.IsNull()) { return Expression{}; }
        bool is_zero = false;
        if (val.type == ValueType::kInt64) {
          is_zero = val.value.int_value == 0;
        } else if (val.type == ValueType::kDouble) {
          is_zero = val.value.double_value == 0.0;
        }
        if (!is_zero) { return Expression{}; }
        return ConstantValueExp(Value());
      }));

    // COALESCE(COALESCE(a, b), c) -> COALESCE(a, b, c)
    built.Add(ExpressionRule(
      "coalesce_flatten",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "coalesce" || fn.Args().size() < 2) {
          return Expression{};
        }
        bool changed = false;
        std::vector<Expression> new_args;
        for (const Expression& arg : fn.Args()) {
          if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
            const auto& inner = arg->AsFunctionCallExpression();
            if (inner.FuncName() == "coalesce") {
              for (const Expression& inner_arg : inner.Args()) {
                new_args.push_back(inner_arg);
              }
              changed = true;
              continue;
            }
          }
          new_args.push_back(arg);
        }
        if (!changed || new_args.size() < 2) { return Expression{}; }
        return FunctionCallExp("coalesce", std::move(new_args));
      }));

    // IF(condition, then, else) -> CASE WHEN condition THEN then ELSE else END
    // Only for 3-argument IF. Canonical form for further rewrite.
    built.Add(ExpressionRule(
      "if_to_case",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "if" || fn.Args().size() != 3) {
          return Expression{};
        }
        return CaseExpressionExp(
            {{fn.Args()[0], fn.Args()[1]}}, fn.Args()[2]);
      }));
    built.Add(ExpressionRule(
      "if_to_case",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "if" || fn.Args().size() != 3) {
          return Expression{};
        }
        return CaseExpressionExp(
            {{fn.Args()[0], fn.Args()[1]}}, fn.Args()[2]);
      }));

    // x __bit_and 0 -> 0
    built.Add(ExpressionRule(
      "bit_and_zero",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "__bit_and" || fn.Args().size() != 2) {
          return Expression{};
        }
        for (const Expression& arg : fn.Args()) {
          if (arg && arg->Type() == TypeTag::kConstantValue) {
            const Value val = arg->AsConstantValue().GetValue();
            if (!val.IsNull() && val.type == ValueType::kInt64 &&
                val.value.int_value == 0) {
              return ConstantValueExp(Value(int64_t(0)));
            }
          }
        }
        return Expression{};
      }));

    // x __bit_or 0 -> x
    built.Add(ExpressionRule(
      "bit_or_zero",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "__bit_or" || fn.Args().size() != 2) {
          return Expression{};
        }
        for (size_t i = 0; i < 2; ++i) {
          if (fn.Args()[i] && fn.Args()[i]->Type() == TypeTag::kConstantValue) {
            const Value val = fn.Args()[i]->AsConstantValue().GetValue();
            if (!val.IsNull() && val.type == ValueType::kInt64 &&
                val.value.int_value == 0) {
              return fn.Args()[1 - i];
            }
          }
        }
        return Expression{};
      }));

    // safe_add(x, 0) -> x, safe_add(0, x) -> x
    built.Add(ExpressionRule(
      "safe_add_zero",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if ((fn.FuncName() != "safe_add" || fn.Args().size() != 2)) {
          return Expression{};
        }
        for (size_t i = 0; i < 2; ++i) {
          if (fn.Args()[i] && fn.Args()[i]->Type() == TypeTag::kConstantValue) {
            const Value val = fn.Args()[i]->AsConstantValue().GetValue();
            if (!val.IsNull()) {
              bool is_zero = (val.type == ValueType::kInt64 && val.value.int_value == 0) ||
                             (val.type == ValueType::kDouble && val.value.double_value == 0.0);
              if (is_zero) { return fn.Args()[1 - i]; }
            }
          }
        }
        return Expression{};
      }));

    // safe_subtract(x, 0) -> x
    built.Add(ExpressionRule(
      "safe_subtract_zero",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "safe_subtract" || fn.Args().size() != 2) {
          return Expression{};
        }
        if (fn.Args()[1] && fn.Args()[1]->Type() == TypeTag::kConstantValue) {
          const Value val = fn.Args()[1]->AsConstantValue().GetValue();
          if (!val.IsNull()) {
            bool is_zero = (val.type == ValueType::kInt64 && val.value.int_value == 0) ||
                           (val.type == ValueType::kDouble && val.value.double_value == 0.0);
            if (is_zero) { return fn.Args()[0]; }
          }
        }
        return Expression{};
      }));

    // safe_multiply(x, 1) -> x, safe_multiply(1, x) -> x
    built.Add(ExpressionRule(
      "safe_multiply_one",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "safe_multiply" || fn.Args().size() != 2) {
          return Expression{};
        }
        for (size_t i = 0; i < 2; ++i) {
          if (fn.Args()[i] && fn.Args()[i]->Type() == TypeTag::kConstantValue) {
            const Value val = fn.Args()[i]->AsConstantValue().GetValue();
            if (!val.IsNull()) {
              bool is_one = (val.type == ValueType::kInt64 && val.value.int_value == 1) ||
                            (val.type == ValueType::kDouble && val.value.double_value == 1.0);
              if (is_one) { return fn.Args()[1 - i]; }
            }
          }
        }
        return Expression{};
      }));

    // safe_multiply(x, 0) -> 0, safe_multiply(0, x) -> 0
    built.Add(ExpressionRule(
      "safe_multiply_zero",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "safe_multiply" || fn.Args().size() != 2) {
          return Expression{};
        }
        for (size_t i = 0; i < 2; ++i) {
          if (fn.Args()[i] && fn.Args()[i]->Type() == TypeTag::kConstantValue) {
            const Value val = fn.Args()[i]->AsConstantValue().GetValue();
            if (!val.IsNull()) {
              bool is_zero = (val.type == ValueType::kInt64 && val.value.int_value == 0) ||
                             (val.type == ValueType::kDouble && val.value.double_value == 0.0);
              if (is_zero) { return ConstantValueExp(Value(int64_t(0))); }
            }
          }
        }
        return Expression{};
      }));

    // ABS(ABS(x)) -> ABS(x)
    built.Add(ExpressionRule(
      "abs_of_abs",
      Is(TypeTag::kFunctionCallExp, "expr"),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& fn = expression->AsFunctionCallExpression();
        if (fn.FuncName() != "abs" || fn.Args().size() != 1) {
          return Expression{};
        }
        if (!fn.Args()[0] ||
            fn.Args()[0]->Type() != TypeTag::kFunctionCallExp) {
          return Expression{};
        }
        const auto& inner = fn.Args()[0]->AsFunctionCallExpression();
        if (inner.FuncName() != "abs") { return Expression{}; }
        return fn.Args()[0];
      }));

    // empty IN list -> FALSE
    built.Add(ExpressionRule(
      "empty_in_list",
      Is(TypeTag::kInExp),
      [](const Expression& expression, const ExpressionBindings&) {
        const auto& in = expression->AsInExpression();
        if (!in.list_.empty()) { return Expression{}; }
        return ConstantValueExp(Value(false));
      }));

    return built;
  }();
  return rules;
}

Expression ExpressionRewriter::Rewrite(const Expression& expression) const {
  Expression current = expression;
  for (size_t pass = 0; pass < 32; ++pass) {
    Expression next = RewriteOnce(current, 0);
    if (Same(current, next)) { return next;
}
    current = std::move(next);
  }
  throw std::runtime_error("expression rewrite did not converge");
}

Expression ExpressionRewriter::RewriteOnce(  // NOLINT(misc-no-recursion)
    const Expression& expression, size_t depth) const {
  if (!expression) { return nullptr;
}
  if (depth >= kMaxRewriteDepth) {
    throw std::runtime_error("expression too deep");
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool children_changed = false;
  for (Expression& child : children) {
    Expression rewritten = RewriteOnce(child, depth + 1);
    children_changed |= !Same(child, rewritten);
    child = std::move(rewritten);
  }
  Expression current =
      children_changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
  for (const ExpressionRule& rule : rules_->Rules()) {
    if (Expression replacement = rule.Apply(current)) { return replacement;
}
  }
  return current;
}

std::vector<Expression> ExpressionChildren(const Expression& expression) {
  if (!expression) { return {};
}
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return {binary.Left(), binary.Right()};
    }
    case TypeTag::kUnaryExp:
      return {expression->AsUnaryExpression().Child()};
    case TypeTag::kAggregateExp:
      return {expression->AsAggregateExpression().Child()};
    case TypeTag::kCaseExp: {
      const auto& case_expression = expression->AsCaseExpression();
      std::vector<Expression> children;
      children.reserve((case_expression.when_clauses_.size() * 2) + 1);
      for (const auto& [condition, result] : case_expression.when_clauses_) {
        children.push_back(condition);
        children.push_back(result);
      }
      if (case_expression.else_clause_) {
        children.push_back(case_expression.else_clause_);
      }
      return children;
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> children{in.child_};
      children.insert(children.end(), in.list_.begin(), in.list_.end());
      return children;
    }
    case TypeTag::kFunctionCallExp:
      return expression->AsFunctionCallExpression().Args();
    case TypeTag::kArrayExp:
      return expression->AsArrayExpression().Elements();
    case TypeTag::kCastExp:
      return {expression->AsCastExpression().Child()};
    case TypeTag::kQueryExp: {
      const auto& test = expression->AsQueryExpression().Test();
      return test ? std::vector<Expression>{test} : std::vector<Expression>{};
    }
    default:
      return {};
  }
}

Expression WithExpressionChildren(const Expression& expression,
                                  std::vector<Expression> children) {
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      if (children.size() != 2) { throw std::invalid_argument("binary arity");
}
      return BinaryExpressionExp(std::move(children[0]),
                                 expression->AsBinaryExpression().Op(),
                                 std::move(children[1]));
    }
    case TypeTag::kUnaryExp:
      if (children.size() != 1) { throw std::invalid_argument("unary arity");
}
      return UnaryExpressionExp(std::move(children[0]),
                                expression->AsUnaryExpression().Op());
    case TypeTag::kAggregateExp: {
      if (children.size() != 1) { throw std::invalid_argument("aggregate arity");
}
      const auto& aggregate = expression->AsAggregateExpression();
      return AggregateExpressionExp(aggregate.GetType(), std::move(children[0]),
                                    aggregate.Distinct());
    }
    case TypeTag::kCaseExp: {
      const auto& source = expression->AsCaseExpression();
      const size_t required =
          (source.when_clauses_.size() * 2) + (source.else_clause_ ? 1 : 0);
      if (children.size() != required) {
        throw std::invalid_argument("case arity");
}
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(source.when_clauses_.size());
      size_t offset = 0;
      for (size_t i = 0; i < source.when_clauses_.size(); ++i) {
        clauses.emplace_back(std::move(children[offset]),
                             std::move(children[offset + 1]));
        offset += 2;
      }
      Expression otherwise =
          source.else_clause_ ? std::move(children[offset]) : Expression{};
      return CaseExpressionExp(std::move(clauses), std::move(otherwise));
    }
    case TypeTag::kInExp: {
      if (children.empty()) { throw std::invalid_argument("in arity");
}
      Expression child = std::move(children.front());
      children.erase(children.begin());
      return InExpressionExp(std::move(child), std::move(children));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(expression->AsFunctionCallExpression().FuncName(),
                             std::move(children));
    case TypeTag::kArrayExp:
      return ArrayExpressionExp(
          std::move(children),
          expression->AsArrayExpression().ElementSqlType());
    case TypeTag::kCastExp: {
      if (children.size() != 1) { throw std::invalid_argument("cast arity");
}
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(std::move(children[0]), cast.TargetTypeName(),
                               cast.ReturnNullOnError());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      if (children.size() > 1) { throw std::invalid_argument("query arity");
}
      return QueryExpressionExp(query.Query(),
                                children.empty() ? nullptr : children.front(),
                                query.Exists(), query.Negated());
    }
    default:
      if (!children.empty()) { throw std::invalid_argument("leaf has children");
}
      return expression;
  }
}

std::vector<Expression> SplitConjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) { return {};
}
  if (expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
    std::vector<Expression> left =
        SplitConjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitConjuncts(expression->AsBinaryExpression().Right());
    left.insert(left.end(), right.begin(), right.end());
    return left;
  }
  return {expression};
}

Expression CombineConjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) { return ConstantValueExp(Value(true));
}
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(std::move(result), BinaryOperation::kAnd,
                                 expressions[i]);
  }
  return result;
}

bool ReferencesOnly(const Expression& expression,
                    const std::unordered_set<std::string>& relation_names) {
  if (!expression) { return true;
}
  return std::ranges::all_of(expression->TouchedColumns(),
                             [&](const ColumnName& column) {
                               return !column.schema.empty() &&
                                      relation_names.contains(column.schema);
                             });
}

}  // namespace tinylamb
