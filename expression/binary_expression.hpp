//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_BINARY_EXPRESSION_HPP
#define TINYLAMB_BINARY_EXPRESSION_HPP

#include <memory>
#include <utility>

#include "expression/expression.hpp"

namespace tinylamb {

class BinaryExpression : public ExpressionBase {
 public:
  BinaryExpression(Expression left, BinaryOperation op, Expression right)
      : left_(std::move(left)), right_(std::move(right)), operation_(op) {}
  [[nodiscard]] TypeTag Type() const override { return TypeTag::kBinaryExp; }
  Value Evaluate(const Row& row, Schema* schema) const override;
  void Dump(std::ostream& o) const override;
  [[nodiscard]] Expression Left() const { return left_; }
  [[nodiscard]] BinaryOperation Op() const { return operation_; }
  [[nodiscard]] Expression Right() const { return right_; }

 private:
  Expression left_;
  Expression right_;
  BinaryOperation operation_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BINARY_EXPRESSION_HPP
