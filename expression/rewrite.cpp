/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

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
  if (!IsConstant(expression)) return false;
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) return false;
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

bool ExpressionPattern::Match(const Expression& expression,
                              ExpressionBindings* bindings) const {
  if (!expression) return false;
  if (type_ && expression->Type() != *type_) return false;
  if (binary_operation_ &&
      expression->AsBinaryExpression().Op() != *binary_operation_) {
    return false;
  }
  if (unary_operation_ &&
      expression->AsUnaryExpression().Op() != *unary_operation_) {
    return false;
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  if (!children_.empty() && children.size() != children_.size()) return false;

  ExpressionBindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, expression);
    if (!inserted && !Same(iter->second, expression)) return false;
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].Match(children[i], &local)) return false;
  }
  *bindings = std::move(local);
  return true;
}

Expression ExpressionRule::Apply(const Expression& expression) const {
  ExpressionBindings bindings;
  if (!pattern_.Match(expression, &bindings)) return nullptr;
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
        if (!std::ranges::all_of(children, IsConstant)) return Expression{};
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
        if (in.list_.size() != 1) return Expression{};
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
          if (has_left && left) return bindings.at("right");
          if (has_right && right) return bindings.at("left");
          if ((has_left && !left) || (has_right && !right)) {
            return ConstantValueExp(Value(false));
          }
        }
        if (operation == BinaryOperation::kOr) {
          if (has_left && !left) return bindings.at("right");
          if (has_right && !right) return bindings.at("left");
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
          if (value.IsNull() || !value.Truthy()) continue;
          otherwise = result;
          break;
        }
        if (!changed) return Expression{};
        if (clauses.empty()) {
          return otherwise ? otherwise : ConstantValueExp(Value());
        }
        return CaseExpressionExp(std::move(clauses), std::move(otherwise));
      }));
    return built;
  }();
  return rules;
}

Expression ExpressionRewriter::Rewrite(const Expression& expression) const {
  Expression current = expression;
  for (size_t pass = 0; pass < 32; ++pass) {
    Expression next = RewriteOnce(current);
    if (Same(current, next)) return next;
    current = std::move(next);
  }
  throw std::runtime_error("expression rewrite did not converge");
}

Expression ExpressionRewriter::RewriteOnce(const Expression& expression) const {
  if (!expression) return nullptr;
  std::vector<Expression> children = ExpressionChildren(expression);
  bool children_changed = false;
  for (Expression& child : children) {
    Expression rewritten = RewriteOnce(child);
    children_changed |= !Same(child, rewritten);
    child = std::move(rewritten);
  }
  Expression current =
      children_changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
  for (const ExpressionRule& rule : rules_->Rules()) {
    if (Expression replacement = rule.Apply(current)) return replacement;
  }
  return current;
}

std::vector<Expression> ExpressionChildren(const Expression& expression) {
  if (!expression) return {};
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
      children.reserve(case_expression.when_clauses_.size() * 2 + 1);
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
      if (children.size() != 2) throw std::invalid_argument("binary arity");
      return BinaryExpressionExp(std::move(children[0]),
                                 expression->AsBinaryExpression().Op(),
                                 std::move(children[1]));
    }
    case TypeTag::kUnaryExp:
      if (children.size() != 1) throw std::invalid_argument("unary arity");
      return UnaryExpressionExp(std::move(children[0]),
                                expression->AsUnaryExpression().Op());
    case TypeTag::kAggregateExp: {
      if (children.size() != 1) throw std::invalid_argument("aggregate arity");
      const auto& aggregate = expression->AsAggregateExpression();
      return AggregateExpressionExp(aggregate.GetType(), std::move(children[0]),
                                    aggregate.Distinct());
    }
    case TypeTag::kCaseExp: {
      const auto& source = expression->AsCaseExpression();
      const size_t required =
          source.when_clauses_.size() * 2 + (source.else_clause_ ? 1 : 0);
      if (children.size() != required)
        throw std::invalid_argument("case arity");
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
      if (children.empty()) throw std::invalid_argument("in arity");
      Expression child = std::move(children.front());
      children.erase(children.begin());
      return InExpressionExp(std::move(child), std::move(children));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(expression->AsFunctionCallExpression().FuncName(),
                             std::move(children));
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      if (children.size() > 1) throw std::invalid_argument("query arity");
      return QueryExpressionExp(query.Query(),
                                children.empty() ? nullptr : children.front(),
                                query.Exists(), query.Negated());
    }
    default:
      if (!children.empty()) throw std::invalid_argument("leaf has children");
      return expression;
  }
}

std::vector<Expression> SplitConjuncts(const Expression& expression) {
  if (!expression) return {};
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
  if (expressions.empty()) return ConstantValueExp(Value(true));
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(std::move(result), BinaryOperation::kAnd,
                                 expressions[i]);
  }
  return result;
}

bool ReferencesOnly(const Expression& expression,
                    const std::unordered_set<std::string>& relation_names) {
  if (!expression) return true;
  for (const ColumnName& column : expression->TouchedColumns()) {
    if (column.schema.empty() || !relation_names.contains(column.schema)) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb
