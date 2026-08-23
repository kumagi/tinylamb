/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_REWRITE_HPP
#define TINYLAMB_EXPRESSION_REWRITE_HPP

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

using ExpressionBindings = std::unordered_map<std::string, Expression>;

// A small C++ pattern DSL used by scalar rewrite rules. Patterns are immutable
// values, so a rule has no dependency on the rewriter or on other rules.
class ExpressionPattern {
 public:
  static ExpressionPattern Any(std::string capture = {});
  static ExpressionPattern Type(TypeTag type, std::string capture = {});
  static ExpressionPattern Binary(std::optional<BinaryOperation> operation,
                                  ExpressionPattern left,
                                  ExpressionPattern right,
                                  std::string capture = {});
  static ExpressionPattern Unary(std::optional<UnaryOperation> operation,
                                 ExpressionPattern child,
                                 std::string capture = {});

  [[nodiscard]] bool Match(const Expression& expression,
                           ExpressionBindings* bindings) const;

 private:
  std::optional<TypeTag> type_;
  std::optional<BinaryOperation> binary_operation_;
  std::optional<UnaryOperation> unary_operation_;
  std::vector<ExpressionPattern> children_;
  std::string capture_;
};

namespace expression_dsl {
inline ExpressionPattern Any(std::string capture = {}) {
  return ExpressionPattern::Any(std::move(capture));
}
inline ExpressionPattern Is(TypeTag type, std::string capture = {}) {
  return ExpressionPattern::Type(type, std::move(capture));
}
inline ExpressionPattern Binary(BinaryOperation operation,
                                ExpressionPattern left, ExpressionPattern right,
                                std::string capture = {}) {
  return ExpressionPattern::Binary(operation, std::move(left), std::move(right),
                                   std::move(capture));
}
inline ExpressionPattern AnyBinary(ExpressionPattern left,
                                   ExpressionPattern right,
                                   std::string capture = {}) {
  return ExpressionPattern::Binary(std::nullopt, std::move(left),
                                   std::move(right), std::move(capture));
}
inline ExpressionPattern Unary(UnaryOperation operation,
                               ExpressionPattern child,
                               std::string capture = {}) {
  return ExpressionPattern::Unary(operation, std::move(child),
                                  std::move(capture));
}
inline ExpressionPattern AnyUnary(ExpressionPattern child,
                                  std::string capture = {}) {
  return ExpressionPattern::Unary(std::nullopt, std::move(child),
                                  std::move(capture));
}
}  // namespace expression_dsl

class ExpressionRule {
 public:
  using Rewrite =
      std::function<Expression(const Expression&, const ExpressionBindings&)>;

  ExpressionRule(std::string name, ExpressionPattern pattern, Rewrite rewrite)
      : name_(std::move(name)),
        pattern_(std::move(pattern)),
        rewrite_(std::move(rewrite)) {}

  [[nodiscard]] const std::string& Name() const { return name_; }
  [[nodiscard]] Expression Apply(const Expression& expression) const;

 private:
  std::string name_;
  ExpressionPattern pattern_;
  Rewrite rewrite_;
};

class ExpressionRuleSet {
 public:
  ExpressionRuleSet& Add(ExpressionRule rule);
  bool Remove(std::string_view name);
  [[nodiscard]] bool Contains(std::string_view name) const;
  [[nodiscard]] const std::vector<ExpressionRule>& Rules() const {
    return rules_;
  }

  static const ExpressionRuleSet& Default();

 private:
  std::vector<ExpressionRule> rules_;
};

class ExpressionRewriter {
 public:
  explicit ExpressionRewriter(const ExpressionRuleSet& rules)
      : rules_(&rules) {}

  [[nodiscard]] Expression Rewrite(const Expression& expression) const;

 private:
  [[nodiscard]] Expression RewriteOnce(const Expression& expression,
                                       size_t depth) const;
  const ExpressionRuleSet* rules_;
};

[[nodiscard]] std::vector<Expression> ExpressionChildren(
    const Expression& expression);
[[nodiscard]] Expression WithExpressionChildren(
    const Expression& expression, std::vector<Expression> children);
[[nodiscard]] std::vector<Expression> SplitConjuncts(
    const Expression& expression);
[[nodiscard]] Expression CombineConjuncts(
    const std::vector<Expression>& expressions);
[[nodiscard]] bool ReferencesOnly(
    const Expression& expression,
    const std::unordered_set<std::string>& relation_names);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_REWRITE_HPP
