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

Value BinaryExpression::Evaluate(const Row& row, const Schema& schema) const {
  return Execute(operation_, left_->Evaluate(row, schema),
                 right_->Evaluate(row, schema));
}

std::string BinaryExpression::ToString() const {
  std::stringstream ss;
  ss << left_->ToString();
  switch (operation_) {
    case BinaryOperation::kAdd:
      ss << " + ";
      break;
    case BinaryOperation::kSubtract:
      ss << " - ";
      break;
    case BinaryOperation::kMultiply:
      ss << " * ";
      break;
    case BinaryOperation::kDivide:
      ss << " / ";
      break;
    case BinaryOperation::kModulo:
      ss << " % ";
      break;
    case BinaryOperation::kEquals:
      ss << " = ";
      break;
    case BinaryOperation::kNotEquals:
      ss << " != ";
      break;
    case BinaryOperation::kLessThan:
      ss << " < ";
      break;
    case BinaryOperation::kLessThanEquals:
      ss << " <= ";
      break;
    case BinaryOperation::kGreaterThan:
      ss << " > ";
      break;
    case BinaryOperation::kGreaterThanEquals:
      ss << " >= ";
      break;
    case BinaryOperation::kAnd:
      ss << " AND ";
      break;
    case BinaryOperation::kOr:
      ss << " OR ";
      break;
    case BinaryOperation::kXor:
      ss << " XOR ";
      break;
  }
  ss << right_->ToString();
  return ss.str();
}

void BinaryExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb