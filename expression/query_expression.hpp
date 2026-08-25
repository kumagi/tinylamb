/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_EXPRESSION_HPP
#define TINYLAMB_QUERY_EXPRESSION_HPP

#include <memory>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"

namespace tinylamb {

class EvaluationContext;
class SelectStatement;

// Three-valued ANY/ALL kernel shared by the AST and relational interpreters:
// ANY is a three-valued OR over per-row comparisons, ALL its dual.  An empty
// row set is vacuous (ALL -> TRUE, ANY -> FALSE).
Value EvaluateQuantifiedComparison(BinaryOperation op, QuantifierMode mode,
                                   const Value& test,
                                   const std::vector<Value>& rows);

class QueryExpression : public ExpressionBase {
 public:
  QueryExpression(std::shared_ptr<SelectStatement> query, Expression test,
                  bool exists, bool negated)
      : QueryExpression(std::move(query), std::move(test), exists, negated,
                        BinaryOperation::kEquals, QuantifierMode::kIn) {}

  QueryExpression(std::shared_ptr<SelectStatement> query, Expression test,
                  bool exists, bool negated, BinaryOperation op,
                  QuantifierMode mode)
      : query_(std::move(query)),
        test_(std::move(test)),
        exists_(exists),
        negated_(negated),
        op_(op),
        mode_(mode) {}

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
  // ARRAY(SELECT ...) mode: evaluation collects every projected row into a
  // single array value instead of scalar/EXISTS/IN semantics.
  [[nodiscard]] bool ArrayResult() const { return array_result_; }
  void SetArrayResult(bool array_result) { array_result_ = array_result; }
  [[nodiscard]] BinaryOperation Op() const { return op_; }
  [[nodiscard]] QuantifierMode Mode() const { return mode_; }

 private:
  std::shared_ptr<SelectStatement> query_;
  Expression test_;
  bool exists_{false};
  bool negated_{false};
  bool array_result_{false};
  BinaryOperation op_{BinaryOperation::kEquals};
  QuantifierMode mode_{QuantifierMode::kIn};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_EXPRESSION_HPP
