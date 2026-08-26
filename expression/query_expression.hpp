/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_EXPRESSION_HPP
#define TINYLAMB_QUERY_EXPRESSION_HPP

#include <memory>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

class EvaluationContext;
class SelectStatement;

class QueryExpression : public ExpressionBase {
 public:
  QueryExpression(std::shared_ptr<SelectStatement> query, Expression test,
                  bool exists, bool negated)
      : query_(std::move(query)),
        test_(std::move(test)),
        exists_(exists),
        negated_(negated) {}

  // Quantified comparison over a subquery: `<test> <compare_op_> {ANY|ALL}
  // (<query>)`.  An empty compare_op_ keeps plain IN semantics.
  QueryExpression(std::shared_ptr<SelectStatement> query, Expression test,
                  std::string compare_op, bool any_mode)
      : query_(std::move(query)),
        test_(std::move(test)),
        compare_op_(std::move(compare_op)),
        any_mode_(any_mode) {}

  // ARRAY(SELECT ...): the projection collected into a single array value.
  static std::shared_ptr<QueryExpression> ArraySubquery(
      std::shared_ptr<SelectStatement> query) {
    auto expr = std::shared_ptr<QueryExpression>(
        new QueryExpression(std::move(query), nullptr, false, false));
    expr->array_mode_ = true;
    return expr;
  }

  [[nodiscard]] TypeTag Type() const override { return TypeTag::kQueryExp; }
  [[nodiscard]] Value Evaluate(const Row&, const Schema&) const override;
  // Stage 1 of the A1 migration: subquery execution goes through the abstract
  // EvaluationContext instead of the relational_detail interpreter.
  [[nodiscard]] Value Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const override;
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& output) const override;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

  [[nodiscard]] const std::shared_ptr<SelectStatement>& Query() const {
    return query_;
  }
  [[nodiscard]] const Expression& Test() const { return test_; }
  [[nodiscard]] bool Exists() const { return exists_; }
  [[nodiscard]] bool Negated() const { return negated_; }
  [[nodiscard]] const std::string& CompareOp() const { return compare_op_; }
  [[nodiscard]] bool AnyMode() const { return any_mode_; }
  [[nodiscard]] bool ArrayMode() const { return array_mode_; }

 private:
  std::shared_ptr<SelectStatement> query_;
  Expression test_;
  bool exists_{false};
  bool negated_{false};
  std::string compare_op_;
  bool any_mode_{false};
  bool array_mode_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_EXPRESSION_HPP
