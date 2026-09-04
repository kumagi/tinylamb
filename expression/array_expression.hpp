/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_ARRAY_EXPRESSION_HPP
#define TINYLAMB_ARRAY_EXPRESSION_HPP

#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

class ArrayExpression : public ExpressionBase {
 public:
  ArrayExpression(std::vector<Expression> elements,
                  std::string element_sql_type)
      : elements_(std::move(elements)),
        element_sql_type_(std::move(element_sql_type)) {}

  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override;
  [[nodiscard]] Value Evaluate(const Row& row, const Schema& schema,
                               EvaluationContext& context) const override;
  [[nodiscard]] tinylamb::Type ResultType(const Schema&) const override {
    return tinylamb::Type(TypeTag::kArray);
  }
  [[nodiscard]] tinylamb::Type ResultType(const Schema&,
                                          const Schema&) const override {
    return tinylamb::Type(TypeTag::kArray);
  }
  [[nodiscard]] const std::vector<Expression>& Elements() const {
    return elements_;
  }
  [[nodiscard]] const std::string& ElementSqlType() const {
    return element_sql_type_;
  }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kArrayExp; }
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;

 private:
  std::vector<Expression> elements_;
  std::string element_sql_type_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ARRAY_EXPRESSION_HPP
