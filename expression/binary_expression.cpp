//
// Created by kumagi on 2022/02/22.
//

#include "binary_expression.hpp"

namespace tinylamb {
namespace {

Value Execute(OpType op, const Value& left, const Value& right) {
  if (left.type != right.type) throw std::runtime_error("type mismatch");
  switch (op) {
    case OpType::kAdd:
      return left + right;
    case OpType::kSubtract:
      return left - right;
    case OpType::kMultiply:
      return left * right;
    case OpType::kDivide:
      return left / right;
    case OpType::kModulo:
      return left % right;
    case OpType::kEquals:
      return Value(left == right);
    case OpType::kNotEquals:
      return Value(left != right);
    case OpType::kLessThan:
      return Value(left < right);
    case OpType::kLessThanEquals:
      return Value(left <= right);
    case OpType::kGreaterThan:
      return Value(left > right);
    case OpType::kGreaterThanEquals:
      return Value(left >= right);
  }
}

}  // anonymous namespace

Value BinaryExpression::Evaluate(const Row& row, Schema* schema) const {
  return Execute(operation_, left_->Evaluate(row, schema),
                 right_->Evaluate(row, schema));
}

}  // namespace tinylamb