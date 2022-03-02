//
// Created by kumagi on 2022/02/22.
//

#include "binary_expression.hpp"

namespace tinylamb {
namespace {

Value Execute(BinaryOperation op, const Value& left, const Value& right) {
  if (left.type != right.type) throw std::runtime_error("type mismatch");
  switch (op) {
    case BinaryOperation::kAdd:
      return left + right;
    case BinaryOperation::kSubtract:
      return left - right;
    case BinaryOperation::kMultiply:
      return left * right;
    case BinaryOperation::kDivide:
      return left / right;
    case BinaryOperation::kModulo:
      return left % right;
    case BinaryOperation::kEquals:
      return Value(left == right);
    case BinaryOperation::kNotEquals:
      return Value(left != right);
    case BinaryOperation::kLessThan:
      return Value(left < right);
    case BinaryOperation::kLessThanEquals:
      return Value(left <= right);
    case BinaryOperation::kGreaterThan:
      return Value(left > right);
    case BinaryOperation::kGreaterThanEquals:
      return Value(left >= right);
    case BinaryOperation::kAnd:
      return Value(left & right);
    case BinaryOperation::kOr:
      return Value(left | right);
    case BinaryOperation::kXor:
      return Value(left ^ right);
  }
}

}  // anonymous namespace

Value BinaryExpression::Evaluate(const Row& row, Schema* schema) const {
  return Execute(operation_, left_->Evaluate(row, schema),
                 right_->Evaluate(row, schema));
}

void BinaryExpression::Dump(std::ostream& o) const {
  left_->Dump(o);
  switch (operation_) {
    case BinaryOperation::kAdd:
      o << " + ";
      break;
    case BinaryOperation::kSubtract:
      o << " - ";
      break;
    case BinaryOperation::kMultiply:
      o << " * ";
      break;
    case BinaryOperation::kDivide:
      o << " / ";
      break;
    case BinaryOperation::kModulo:
      o << " % ";
      break;
    case BinaryOperation::kEquals:
      o << " = ";
      break;
    case BinaryOperation::kNotEquals:
      o << " != ";
      break;
    case BinaryOperation::kLessThan:
      o << " < ";
      break;
    case BinaryOperation::kLessThanEquals:
      o << " <= ";
      break;
    case BinaryOperation::kGreaterThan:
      o << " > ";
      break;
    case BinaryOperation::kGreaterThanEquals:
      o << " >= ";
      break;
    case BinaryOperation::kAnd:
      o << " AND ";
      break;
    case BinaryOperation::kOr:
      o << " OR ";
      break;
    case BinaryOperation::kXor:
      o << " XOR ";
      break;
  }
  right_->Dump(o);
}

}  // namespace tinylamb