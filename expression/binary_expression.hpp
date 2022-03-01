//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_BINARY_EXPRESSION_HPP
#define TINYLAMB_BINARY_EXPRESSION_HPP

#include <memory>

#include "expression/expression_base.hpp"

namespace tinylamb {

class BinaryExpression : public ExpressionBase {
  BinaryExpression(std::shared_ptr<ExpressionBase> left, BinaryOperation op,
                   std::shared_ptr<ExpressionBase> right)
      : left_(std::move(left)), right_(std::move(right)), operation_(op) {}

 public:
  Value Evaluate(const Row& row, Schema* schema) const override;
  void Dump(std::ostream& o) const override;

 private:
  friend class Expression;

  std::shared_ptr<ExpressionBase> left_;
  std::shared_ptr<ExpressionBase> right_;
  BinaryOperation operation_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BINARY_EXPRESSION_HPP
