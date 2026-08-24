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

 private:
  std::shared_ptr<SelectStatement> query_;
  Expression test_;
  bool exists_{false};
  bool negated_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_EXPRESSION_HPP
