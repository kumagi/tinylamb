//
// Created by kumagi on 2022/02/22.
//

#ifndef TINYLAMB_BINARY_EXPRESSION_HPP
#define TINYLAMB_BINARY_EXPRESSION_HPP

#include <memory>

#include "expression/expression_base.hpp"

namespace tinylamb {

enum class OpType {
  // Calculations.
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kModulo,

  // Comparisons.
  kEquals,
  kNotEquals,
  kLessThan,
  kLessThanEquals,
  kGreaterThan,
  kGreaterThanEquals,
};

class BinaryExpression : public ExpressionBase {
 public:
  BinaryExpression(std::unique_ptr<ExpressionBase>&& left,
                   std::unique_ptr<ExpressionBase>&& right, OpType op)
      : left_(std::move(left)), right_(std::move(right)), operation_(op) {}
  Value Evaluate(const Row& row, Schema* schema) const override;

 private:
  std::unique_ptr<ExpressionBase> left_;
  std::unique_ptr<ExpressionBase> right_;
  OpType operation_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BINARY_EXPRESSION_HPP
