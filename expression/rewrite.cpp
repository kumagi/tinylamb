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
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
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
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
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
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
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
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  return !constant.IsNull() && constant.type == ValueType::kInt64;
}

bool HasLikeWildcard(std::string_view pattern) {
  return pattern.find('%') != std::string_view::npos ||
         pattern.find('_') != std::string_view::npos;
}

bool IsNondeterministicFunction(std::string_view name) {
  return name == "current_timestamp" || name == "current_date" ||
         name == "current_time" || name == "now" || name == "rand" ||
         name == "random" || name == "generate_uuid" || name == "new_uuid";
}

bool IsBooleanExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kBinaryExp) {
    const auto operation = expression->AsBinaryExpression().Op();
    return operation == BinaryOperation::kAnd ||
           operation == BinaryOperation::kOr ||
           operation == BinaryOperation::kXor || IsComparison(operation);
  }
  if (expression->Type() == TypeTag::kUnaryExp) {
    const auto operation = expression->AsUnaryExpression().Op();
    return operation == UnaryOperation::kNot ||
           operation == UnaryOperation::kIsNull ||
           operation == UnaryOperation::kIsNotNull;
  }
  return false;
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
  if (!expression) {
    return false;
  }
  if (type_ && expression->Type() != *type_) {
    return false;
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
  if (!children_.empty() && children.size() != children_.size()) {
    return false;
  }

  ExpressionBindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, expression);
    if (!inserted && !Same(iter->second, expression)) {
      return false;
    }
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].Match(children[i], &local)) {
      return false;
    }
  }
  *bindings = std::move(local);
  return true;
}

Expression ExpressionRule::Apply(const Expression& expression) const {
  ExpressionBindings bindings;
  if (!pattern_.Match(expression, &bindings)) {
    return nullptr;
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
    // For a stable column reference, `x = x` is true exactly when x is not
    // NULL.  Restrict this rewrite to columns: evaluating a volatile or
    // otherwise expensive expression twice is not interchangeable with an
    // IS NOT NULL test under SQL's expression semantics.
    built.Add(ExpressionRule(
        "comparison_of_same_column", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          if (expression->AsBinaryExpression().Op() !=
                  BinaryOperation::kEquals ||
              !Same(left, right) || left->Type() != TypeTag::kColumnValue) {
            return Expression{};
          }
          return UnaryExpressionExp(left, UnaryOperation::kIsNotNull);
        }));
    built.Add(ExpressionRule(
        "self_strict_inequality", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto operation = expression->AsBinaryExpression().Op();
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          if ((operation != BinaryOperation::kLessThan &&
               operation != BinaryOperation::kGreaterThan) ||
              left->Type() != TypeTag::kColumnValue || !Same(left, right)) {
            return Expression{};
          }
          // x < x is FALSE for non-NULL x and UNKNOWN for NULL x.  A bare
          // FALSE would incorrectly reject the UNKNOWN case in contexts that
          // preserve three-valued results, so retain the null branch
          // explicitly.
          return CaseExpressionExp(
              {{UnaryExpressionExp(left, UnaryOperation::kIsNull),
                ConstantValueExp(Value())}},
              ConstantValueExp(Value(false)));
        }));
    built.Add(ExpressionRule(
        "range_predicate_merge",
        Binary(BinaryOperation::kAnd, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& lhs = bindings.at("left");
          const Expression& rhs = bindings.at("right");
          if (lhs->Type() != TypeTag::kBinaryExp ||
              rhs->Type() != TypeTag::kBinaryExp) {
            return Expression{};
          }
          const auto& left = lhs->AsBinaryExpression();
          const auto& right = rhs->AsBinaryExpression();
          if (left.Left()->Type() != TypeTag::kColumnValue ||
              right.Left()->Type() != TypeTag::kColumnValue ||
              !Same(left.Left(), right.Left()) ||
              left.Right()->Type() != TypeTag::kConstantValue ||
              right.Right()->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          if (left.Op() != right.Op() ||
              (left.Op() != BinaryOperation::kGreaterThan &&
               left.Op() != BinaryOperation::kGreaterThanEquals &&
               left.Op() != BinaryOperation::kLessThan &&
               left.Op() != BinaryOperation::kLessThanEquals)) {
            return Expression{};
          }
          const Value first = left.Right()->AsConstantValue().GetValue();
          const Value second = right.Right()->AsConstantValue().GetValue();
          if (first.IsNull() || second.IsNull()) {
            return Expression{};
          }
          const bool choose_second =
              left.Op() == BinaryOperation::kGreaterThan ||
                      left.Op() == BinaryOperation::kGreaterThanEquals
                  ? first < second
                  : second < first;
          return BinaryExpressionExp(
              left.Left(), left.Op(),
              choose_second ? right.Right() : left.Right());
        }));
    built.Add(ExpressionRule(
        "redundant_filter_removal",
        Binary(BinaryOperation::kAnd, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto implies = [](const Expression& stronger,
                                  const Expression& weaker) {
            if (stronger->Type() != TypeTag::kBinaryExp ||
                weaker->Type() != TypeTag::kBinaryExp) {
              return false;
            }
            const auto& strong = stronger->AsBinaryExpression();
            const auto& weak = weaker->AsBinaryExpression();
            if (strong.Op() != BinaryOperation::kEquals ||
                !IsComparison(weak.Op()) ||
                strong.Left()->Type() != TypeTag::kColumnValue ||
                weak.Left()->Type() != TypeTag::kColumnValue ||
                !Same(strong.Left(), weak.Left()) ||
                strong.Right()->Type() != TypeTag::kConstantValue ||
                weak.Right()->Type() != TypeTag::kConstantValue) {
              return false;
            }
            const Value strong_value =
                strong.Right()->AsConstantValue().GetValue();
            const Value weak_value = weak.Right()->AsConstantValue().GetValue();
            if (strong_value.IsNull() || weak_value.IsNull()) {
              return false;
            }
            try {
              const Value result =
                  EvaluateBinary(weak.Op(), strong_value, weak_value);
              return !result.IsNull() && result.Truthy();
            } catch (const std::exception&) {
              return false;
            }
          };
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          if (implies(left, right)) {
            return left;
          }
          if (implies(right, left)) {
            return right;
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "or_to_in", Binary(BinaryOperation::kOr, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& lhs = bindings.at("left");
          const Expression& rhs = bindings.at("right");
          if (lhs->Type() != TypeTag::kBinaryExp ||
              rhs->Type() != TypeTag::kBinaryExp) {
            return Expression{};
          }
          const auto& left = lhs->AsBinaryExpression();
          const auto& right = rhs->AsBinaryExpression();
          if (left.Op() != BinaryOperation::kEquals ||
              right.Op() != BinaryOperation::kEquals ||
              left.Left()->Type() != TypeTag::kColumnValue ||
              !Same(left.Left(), right.Left()) ||
              right.Right()->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          if (left.Right()->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          return InExpressionExp(left.Left(), {left.Right(), right.Right()});
        }));
    built.Add(ExpressionRule(
        "fold_in", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const std::vector<Expression> children =
              ExpressionChildren(expression);
          if (!std::ranges::all_of(children, IsConstant)) {
            return Expression{};
          }
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "fold_array", Is(TypeTag::kArrayExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& array = expression->AsArrayExpression();
          if (!std::ranges::all_of(array.Elements(), IsConstant)) {
            return Expression{};
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
          if (IsNondeterministicFunction(
                  expression->AsFunctionCallExpression().FuncName())) {
            return Expression{};
          }
          const auto is_literal = [](const Expression& arg) {
            return arg && (arg->Type() == TypeTag::kConstantValue ||
                           arg->Type() == TypeTag::kIntervalExp);
          };
          if (!std::ranges::all_of(ExpressionChildren(expression),
                                   is_literal)) {
            return Expression{};
          }
          try {
            return ConstantValueExp(expression->Evaluate(Row(), Schema()));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "coalesce_flatten", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if (call.FuncName() != "coalesce") {
            return Expression{};
          }
          std::vector<Expression> flattened;
          bool changed = false;
          for (const Expression& argument : call.Args()) {
            if (argument && argument->Type() == TypeTag::kFunctionCallExp &&
                argument->AsFunctionCallExpression().FuncName() == "coalesce") {
              const auto& nested = argument->AsFunctionCallExpression();
              flattened.insert(flattened.end(), nested.Args().begin(),
                               nested.Args().end());
              changed = true;
            } else {
              flattened.push_back(argument);
            }
          }
          if (!changed) {
            return Expression{};
          }
          return FunctionCallExp("coalesce", std::move(flattened));
        }));
    built.Add(ExpressionRule(
        "concat_flatten", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if (call.FuncName() != "concat") {
            return Expression{};
          }
          std::vector<Expression> flattened;
          bool changed = false;
          for (const Expression& argument : call.Args()) {
            if (argument && argument->Type() == TypeTag::kFunctionCallExp &&
                argument->AsFunctionCallExpression().FuncName() == "concat") {
              const auto& nested = argument->AsFunctionCallExpression();
              flattened.insert(flattened.end(), nested.Args().begin(),
                               nested.Args().end());
              changed = true;
            } else {
              flattened.push_back(argument);
            }
          }
          if (!changed) {
            return Expression{};
          }
          return FunctionCallExp("concat", std::move(flattened));
        }));
    built.Add(ExpressionRule(
        "greatest_least_single_arg", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if ((call.FuncName() != "greatest" && call.FuncName() != "least") ||
              call.Args().size() != 1) {
            return Expression{};
          }
          return call.Args().front();
        }));
    built.Add(ExpressionRule(
        "abs_of_abs", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& outer = expression->AsFunctionCallExpression();
          if (outer.FuncName() != "abs" || outer.Args().size() != 1 ||
              outer.Args().front()->Type() != TypeTag::kFunctionCallExp) {
            return Expression{};
          }
          const auto& inner = outer.Args().front()->AsFunctionCallExpression();
          if (inner.FuncName() != "abs" || inner.Args().size() != 1) {
            return Expression{};
          }
          return outer.Args().front();
        }));
    built.Add(ExpressionRule(
        "nullif_to_case", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if (call.FuncName() != "nullif" || call.Args().size() != 2 ||
              call.Args()[0]->Type() != TypeTag::kColumnValue ||
              call.Args()[1]->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          return CaseExpressionExp(
              {{BinaryExpressionExp(call.Args()[0], BinaryOperation::kEquals,
                                    call.Args()[1]),
                ConstantValueExp(Value())}},
              call.Args()[0]);
        }));
    // `a IS [NOT] DISTINCT FROM <constant>` has a canonical three-valued
    // form: NULL on the constant side collapses to an IS (NOT) NULL test,
    // and a non-NULL constant unfolds into the equivalent OR/AND comparison
    // so scan rules can treat it like any other sargable predicate.
    built.Add(ExpressionRule(
        "is_distinct_from_rewrite", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if (call.FuncName() != "__is_distinct_from" ||
              call.Args().size() != 2 ||
              call.Args()[1]->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          const Value& constant = call.Args()[1]->AsConstantValue().GetValue();
          if (constant.IsNull()) {
            return UnaryExpressionExp(call.Args()[0],
                                      UnaryOperation::kIsNotNull);
          }
          return BinaryExpressionExp(
              UnaryExpressionExp(call.Args()[0], UnaryOperation::kIsNull),
              BinaryOperation::kOr,
              BinaryExpressionExp(call.Args()[0], BinaryOperation::kNotEquals,
                                  call.Args()[1]));
        }));
    built.Add(ExpressionRule(
        "not_is_distinct_from_rewrite",
        expression_dsl::Unary(UnaryOperation::kNot,
                              Is(TypeTag::kFunctionCallExp, "idc_call")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto& call =
              bindings.at("idc_call")->AsFunctionCallExpression();
          if (call.FuncName() != "__is_distinct_from" ||
              call.Args().size() != 2 ||
              call.Args()[1]->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          const Value& constant = call.Args()[1]->AsConstantValue().GetValue();
          if (constant.IsNull()) {
            return UnaryExpressionExp(call.Args()[0], UnaryOperation::kIsNull);
          }
          return BinaryExpressionExp(
              UnaryExpressionExp(call.Args()[0], UnaryOperation::kIsNotNull),
              BinaryOperation::kAnd,
              BinaryExpressionExp(call.Args()[0], BinaryOperation::kEquals,
                                  call.Args()[1]));
        }));
    // SAFE_DIVIDE(x, <constant 0>) is NULL by definition; pinning the
    // constant failure case removes the runtime zero check from every row.
    built.Add(ExpressionRule(
        "safe_divide_rewrite", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if (call.FuncName() != "safe_divide" || call.Args().size() != 2 ||
              call.Args()[1]->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          const Value& divisor = call.Args()[1]->AsConstantValue().GetValue();
          if (divisor.IsNull()) {
            return Expression{};
          }
          if ((divisor.type == ValueType::kInt64 &&
               divisor.value.int_value == 0) ||
              (divisor.type == ValueType::kDouble &&
               divisor.value.double_value == 0.0)) {
            return ConstantValueExp(Value());
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "if_to_case", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& call = expression->AsFunctionCallExpression();
          if (call.FuncName() != "if" || call.IsCanonicalIf() ||
              call.Args().size() != 3) {
            return Expression{};
          }
          // CASE evaluates only the selected branch, matching IF's
          // conditional-evaluation contract while exposing a standard scalar
          // form to subsequent rewrite rules.
          return CaseExpressionExp({{call.Args()[0], call.Args()[1]}},
                                   call.Args()[2], true);
        }));
    built.Add(ExpressionRule(
        "simplify_coalesce_null_test", Is(TypeTag::kUnaryExp, "check"),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& expression = bindings.at("check");
          const auto& unary = expression->AsUnaryExpression();
          if (unary.Op() != UnaryOperation::kIsNull &&
              unary.Op() != UnaryOperation::kIsNotNull) {
            return Expression{};
          }
          if (!unary.Child() ||
              unary.Child()->Type() != TypeTag::kFunctionCallExp) {
            return Expression{};
          }
          const auto& call = unary.Child()->AsFunctionCallExpression();
          if (call.FuncName() != "coalesce") {
            return Expression{};
          }
          // A non-NULL literal reached after only NULL literals fixes the
          // result without evaluating later arguments.  Do not apply this to
          // arbitrary expressions: they may throw or be volatile.
          for (const Expression& argument : call.Args()) {
            if (!IsConstant(argument)) {
              return Expression{};
            }
            const Value value = argument->AsConstantValue().GetValue();
            if (value.IsNull()) {
              continue;
            }
            return ConstantValueExp(
                Value(unary.Op() == UnaryOperation::kIsNotNull));
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "in_single_null", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (in.child_->Type() != TypeTag::kColumnValue ||
              in.list_.size() != 1 ||
              in.list_.front()->Type() != TypeTag::kConstantValue ||
              !in.list_.front()->AsConstantValue().GetValue().IsNull()) {
            return Expression{};
          }
          return ConstantValueExp(Value());
        }));
    built.Add(ExpressionRule(
        "singleton_in", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (in.list_.size() != 1) {
            return Expression{};
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
        "sargable_rewrite", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (!IsComparison(binary.Op())) {
            return Expression{};
          }
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          if (!right || right->Type() != TypeTag::kConstantValue || !left ||
              left->Type() != TypeTag::kBinaryExp) {
            return Expression{};
          }
          const auto& arithmetic = left->AsBinaryExpression();
          if (arithmetic.Op() != BinaryOperation::kAdd &&
              arithmetic.Op() != BinaryOperation::kSubtract) {
            return Expression{};
          }
          if (arithmetic.Left()->Type() != TypeTag::kColumnValue ||
              arithmetic.Right()->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          const Value rhs = right->AsConstantValue().GetValue();
          const Value offset = arithmetic.Right()->AsConstantValue().GetValue();
          if (rhs.IsNull() || offset.IsNull() ||
              (rhs.type != ValueType::kInt64 &&
               rhs.type != ValueType::kDouble) ||
              (offset.type != ValueType::kInt64 &&
               offset.type != ValueType::kDouble)) {
            return Expression{};
          }
          try {
            const Value bound =
                EvaluateBinary(arithmetic.Op() == BinaryOperation::kAdd
                                   ? BinaryOperation::kSubtract
                                   : BinaryOperation::kAdd,
                               rhs, offset);
            return BinaryExpressionExp(arithmetic.Left(), binary.Op(),
                                       ConstantValueExp(bound));
          } catch (const std::exception&) {
            return Expression{};
          }
        }));
    built.Add(ExpressionRule(
        "comparison_with_null", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto operation = expression->AsBinaryExpression().Op();
          if (!IsComparison(operation)) {
            return Expression{};
          }
          const auto is_null_constant = [](const Expression& item) {
            return IsConstant(item) &&
                   item->AsConstantValue().GetValue().IsNull();
          };
          if (!is_null_constant(bindings.at("left")) &&
              !is_null_constant(bindings.at("right"))) {
            return Expression{};
          }
          // SQL comparisons involving NULL evaluate to UNKNOWN, represented by
          // the engine's NULL Value.  A Selection can then turn it into Empty.
          return ConstantValueExp(Value());
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
            if (has_left && left) {
              return bindings.at("right");
            }
            if (has_right && right) {
              return bindings.at("left");
            }
            if ((has_left && !left) || (has_right && !right)) {
              return ConstantValueExp(Value(false));
            }
          }
          if (operation == BinaryOperation::kOr) {
            if (has_left && !left) {
              return bindings.at("right");
            }
            if (has_right && !right) {
              return bindings.at("left");
            }
            if ((has_left && left) || (has_right && right)) {
              return ConstantValueExp(Value(true));
            }
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "xor_to_or_and_not",
        Binary(BinaryOperation::kXor, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          const Expression either =
              BinaryExpressionExp(left, BinaryOperation::kOr, right);
          const Expression both =
              BinaryExpressionExp(left, BinaryOperation::kAnd, right);
          return BinaryExpressionExp(
              either, BinaryOperation::kAnd,
              UnaryExpressionExp(both, UnaryOperation::kNot));
        }));
    built.Add(ExpressionRule(
        "boolean_eq_true_false", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto operation = expression->AsBinaryExpression().Op();
          if (operation != BinaryOperation::kEquals &&
              operation != BinaryOperation::kNotEquals) {
            return Expression{};
          }
          Expression boolean;
          Value literal;
          const auto inspect = [&](const Expression& candidate,
                                   const Expression& other) {
            if (!IsBooleanExpression(candidate) ||
                other->Type() != TypeTag::kConstantValue) {
              return false;
            }
            const Value value = other->AsConstantValue().GetValue();
            if (value.IsNull() || value.type != ValueType::kInt64 ||
                (value.value.int_value != 0 && value.value.int_value != 1)) {
              return false;
            }
            boolean = candidate;
            literal = value;
            return true;
          };
          if (!inspect(bindings.at("left"), bindings.at("right")) &&
              !inspect(bindings.at("right"), bindings.at("left"))) {
            return Expression{};
          }
          const bool is_true = literal.value.int_value != 0;
          const bool negate =
              operation == BinaryOperation::kNotEquals ? is_true : !is_true;
          return negate ? UnaryExpressionExp(boolean, UnaryOperation::kNot)
                        : boolean;
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
          const auto operation =
              bindings.at("binary")->AsBinaryExpression().Op();
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
            if (value.IsNull() || !value.Truthy()) {
              continue;
            }
            otherwise = result;
            break;
          }
          if (!changed) {
            return Expression{};
          }
          if (clauses.empty()) {
            return otherwise ? otherwise : ConstantValueExp(Value());
          }
          return CaseExpressionExp(std::move(clauses), std::move(otherwise));
        }));
    built.Add(ExpressionRule(
        "case_to_if", Is(TypeTag::kCaseExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& source = expression->AsCaseExpression();
          if (source.IsFromIf() || source.when_clauses_.size() != 1) {
            return Expression{};
          }
          Expression otherwise = source.else_clause_
                                     ? source.else_clause_
                                     : ConstantValueExp(Value());
          return FunctionCallExp(
              "if",
              {source.when_clauses_[0].first, source.when_clauses_[0].second,
               std::move(otherwise)},
              true);
        }));
    built.Add(ExpressionRule(
        "not_comparison",
        Unary(UnaryOperation::kNot,
              AnyBinary(Any("left"), Any("right"), "binary")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto operation =
              bindings.at("binary")->AsBinaryExpression().Op();
          if (!IsComparison(operation)) {
            return Expression{};
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
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kLike,
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
          if (IsZero(bindings.at("left"))) {
            return bindings.at("right");
          }
          if (IsZero(bindings.at("right"))) {
            return bindings.at("left");
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "identity_subtract_zero",
        Binary(BinaryOperation::kSubtract, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!IsZero(bindings.at("right"))) {
            return Expression{};
          }
          return bindings.at("left");
        }));
    built.Add(ExpressionRule(
        "identity_multiply_one",
        Binary(BinaryOperation::kMultiply, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (IsOne(bindings.at("left"))) {
            return bindings.at("right");
          }
          if (IsOne(bindings.at("right"))) {
            return bindings.at("left");
          }
          return Expression{};
        }));
    built.Add(ExpressionRule(
        "identity_divide_one",
        Binary(BinaryOperation::kDivide, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!IsOne(bindings.at("right"))) {
            return Expression{};
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
          if (!changed) {
            return Expression{};
          }
          return InExpressionExp(in.child_, std::move(items));
        }));
    built.Add(ExpressionRule(
        "in_empty_list", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (!in.list_.empty()) {
            return Expression{};
          }
          return ConstantValueExp(Value(false));
        }));
    built.Add(ExpressionRule(
        "uniform_case_result", Is(TypeTag::kCaseExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& source = expression->AsCaseExpression();
          if (!source.else_clause_) {
            return Expression{};
          }
          for (const auto& [condition, result] : source.when_clauses_) {
            (void)condition;
            if (!Same(result, source.else_clause_)) {
              return Expression{};
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
          if (HasLikeWildcard(pattern.value.varchar_value)) {
            return Expression{};
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
          if (HasLikeWildcard(pattern.value.varchar_value)) {
            return Expression{};
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
          if (binary.Op() != BinaryOperation::kOr) {
            return Expression{};
          }
          const std::vector<Expression> left = SplitConjuncts(binary.Left());
          const std::vector<Expression> right = SplitConjuncts(binary.Right());
          if (left.empty() || right.empty()) {
            return Expression{};
          }
          std::vector<Expression> common;
          std::vector<Expression> left_rest;
          for (const Expression& conjunct : left) {
            const bool shared =
                std::ranges::any_of(right, [&](const Expression& candidate) {
                  return Same(conjunct, candidate);
                });
            (shared ? common : left_rest).push_back(conjunct);
          }
          if (common.empty()) {
            return Expression{};
          }
          std::vector<Expression> right_rest;
          for (const Expression& conjunct : right) {
            const bool shared =
                std::ranges::any_of(common, [&](const Expression& candidate) {
                  return Same(conjunct, candidate);
                });
            if (!shared) {
              right_rest.push_back(conjunct);
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
    return built;
  }();
  return rules;
}

Expression ExpressionRewriter::Rewrite(const Expression& expression) const {
  Expression current = expression;
  for (size_t pass = 0; pass < 32; ++pass) {
    Expression next = RewriteOnce(current, 0);
    if (Same(current, next)) {
      return next;
    }
    current = std::move(next);
  }
  throw std::runtime_error("expression rewrite did not converge");
}

Expression ExpressionRewriter::RewriteOnce(  // NOLINT(misc-no-recursion)
    const Expression& expression, size_t depth) const {
  if (!expression) {
    return nullptr;
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
    if (Expression replacement = rule.Apply(current)) {
      return replacement;
    }
  }
  return current;
}

std::vector<Expression> ExpressionChildren(const Expression& expression) {
  if (!expression) {
    return {};
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
      if (children.size() != 2) {
        throw std::invalid_argument("binary arity");
      }
      return BinaryExpressionExp(std::move(children[0]),
                                 expression->AsBinaryExpression().Op(),
                                 std::move(children[1]));
    }
    case TypeTag::kUnaryExp:
      if (children.size() != 1) {
        throw std::invalid_argument("unary arity");
      }
      return UnaryExpressionExp(std::move(children[0]),
                                expression->AsUnaryExpression().Op());
    case TypeTag::kAggregateExp: {
      if (children.size() != 1) {
        throw std::invalid_argument("aggregate arity");
      }
      const auto& aggregate = expression->AsAggregateExpression();
      auto rebuilt = std::make_shared<AggregateExpression>(
          aggregate.GetType(), std::move(children[0]), aggregate.Distinct());
      if (aggregate.Having() != AggregateHavingModifier::kNone) {
        rebuilt->SetHaving(aggregate.Having(), aggregate.HavingCondition());
      }
      if (aggregate.WhereFilter()) {
        rebuilt->SetWhereFilter(aggregate.WhereFilter());
      }
      if (!aggregate.InnerOrderBy().empty()) {
        rebuilt->SetInnerOrderBy(aggregate.InnerOrderBy());
      }
      if (aggregate.InnerLimit().has_value()) {
        rebuilt->SetInnerLimit(aggregate.InnerLimit());
      }
      if (aggregate.SecondaryArg()) {
        rebuilt->SetSecondaryArg(aggregate.SecondaryArg());
      }
      return rebuilt;
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
      return CaseExpressionExp(std::move(clauses), std::move(otherwise),
                               source.from_if_);
    }
    case TypeTag::kInExp: {
      if (children.empty()) {
        throw std::invalid_argument("in arity");
      }
      Expression child = std::move(children.front());
      children.erase(children.begin());
      return InExpressionExp(std::move(child), std::move(children));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(
          expression->AsFunctionCallExpression().FuncName(),
          std::move(children),
          expression->AsFunctionCallExpression().IsCanonicalIf());
    case TypeTag::kArrayExp:
      return ArrayExpressionExp(
          std::move(children),
          expression->AsArrayExpression().ElementSqlType());
    case TypeTag::kCastExp: {
      if (children.size() != 1) {
        throw std::invalid_argument("cast arity");
      }
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(std::move(children[0]), cast.TargetTypeName(),
                               cast.ReturnNullOnError());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      if (children.size() > 1) {
        throw std::invalid_argument("query arity");
      }
      return QueryExpressionExp(query.Query(),
                                children.empty() ? nullptr : children.front(),
                                query.Exists(), query.Negated());
    }
    default:
      if (!children.empty()) {
        throw std::invalid_argument("leaf has children");
      }
      return expression;
  }
}

std::vector<Expression> SplitConjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return {};
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
  if (expressions.empty()) {
    return ConstantValueExp(Value(true));
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
  if (!expression) {
    return true;
  }
  return std::ranges::all_of(
      expression->TouchedColumns(), [&](const ColumnName& column) {
        return !column.schema.empty() && relation_names.contains(column.schema);
      });
}

}  // namespace tinylamb
