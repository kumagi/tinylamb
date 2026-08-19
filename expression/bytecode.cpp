/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <stdexcept>
#include <limits>

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"

namespace tinylamb {
namespace {

ValueType ValueTypeFor(const Type& type) {
  switch (type.GetType()) {
    case TypeTag::kInteger:
    case TypeTag::kBigInt:
      return ValueType::kInt64;
    case TypeTag::kDouble:
      return ValueType::kDouble;
    case TypeTag::kVarChar:
      return ValueType::kVarChar;
    case TypeTag::kDate:
      return ValueType::kDate;
    default:
      return ValueType::kNull;
  }
}

BytecodeOp BinaryOpcode(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
      return BytecodeOp::kBinaryInt64;
    case ValueType::kDouble:
      return BytecodeOp::kBinaryDouble;
    case ValueType::kVarChar:
      return BytecodeOp::kBinaryVarchar;
    case ValueType::kDate:
      return BytecodeOp::kBinaryDate;
    case ValueType::kNull:
      throw std::runtime_error("untyped bytecode operand");
  }
  throw std::runtime_error("untyped bytecode operand");
}

bool CompileNode(const Expression& expression, const Schema& schema,
                 BytecodeProgram* program) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const int offset =
          schema.Offset(expression->AsColumnValue().GetColumnName());
      if (offset < 0) return false;
      program->AddInstruction(
          {BytecodeOp::kLoadColumn, static_cast<uint16_t>(offset)});
      return true;
    }
    case TypeTag::kConstantValue: {
      const uint16_t offset =
          program->AddConstant(expression->AsConstantValue().GetValue());
      program->AddInstruction({BytecodeOp::kLoadConstant, offset});
      return true;
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      if (!CompileNode(binary.Left(), schema, program) ||
          !CompileNode(binary.Right(), schema, program)) {
        return false;
      }
      ValueType operand_type = ValueTypeFor(binary.Left()->ResultType(schema));
      if (operand_type == ValueType::kDouble ||
          ValueTypeFor(binary.Right()->ResultType(schema)) ==
              ValueType::kDouble) {
        operand_type = ValueType::kDouble;
      }
      program->AddInstruction(
          {BinaryOpcode(operand_type), 0, binary.Op()});
      return true;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expression->AsUnaryExpression();
      if (!CompileNode(unary.Child(), schema, program)) return false;
      const ValueType operand_type = ValueTypeFor(unary.Child()->ResultType(schema));
      if (operand_type != ValueType::kInt64 &&
          operand_type != ValueType::kDouble) {
        return false;
      }
      program->AddInstruction(
          {operand_type == ValueType::kDouble ? BytecodeOp::kUnaryDouble
                                              : BytecodeOp::kUnaryInt64,
           0, BinaryOperation::kAdd, unary.Op()});
      return true;
    }
    default:
      return false;
  }
}

}  // namespace

std::optional<BytecodeProgram> BytecodeCompiler::Compile(
    const Expression& expression, const Schema& schema) {
  try {
    const Expression folded =
        ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(expression);
    BytecodeProgram program;
    if (!CompileNode(folded, schema, &program)) return std::nullopt;
    const ValueType result_type = ValueTypeFor(folded->ResultType(schema));
    if (result_type == ValueType::kNull) return std::nullopt;
    program.SetResultType(result_type);
    return program;
  } catch (const std::exception&) {
    // Plans with partially qualified or deferred schemas remain valid.  An
    // unsupported bytecode shape must fall back to the expression tree.
    return std::nullopt;
  }
}

ColumnVector BytecodeProgram::EvaluateBatch(const DataChunk& input) const {
  ColumnVector result(result_type_, input.Size());
  std::vector<Value> stack;
  stack.reserve(instructions_.size());
  for (size_t row = 0; row < input.Size(); ++row) {
    stack.clear();
    for (const BytecodeInstruction& instruction : instructions_) {
      switch (instruction.opcode) {
        case BytecodeOp::kLoadColumn:
          stack.push_back(input.ColumnAt(instruction.operand).ValueAt(row));
          break;
        case BytecodeOp::kLoadConstant:
          stack.push_back(constants_[instruction.operand]);
          break;
        case BytecodeOp::kBinaryInt64:
        case BytecodeOp::kBinaryDouble:
        case BytecodeOp::kBinaryVarchar:
        case BytecodeOp::kBinaryDate: {
          Value right = std::move(stack.back());
          stack.pop_back();
          Value left = std::move(stack.back());
          stack.pop_back();
          stack.push_back(EvaluateBinary(instruction.binary, left, right));
          break;
        }
        case BytecodeOp::kUnaryInt64:
        case BytecodeOp::kUnaryDouble: {
          Value child = std::move(stack.back());
          stack.pop_back();
          stack.push_back(EvaluateUnary(instruction.unary, std::move(child)));
          break;
        }
      }
    }
    if (stack.size() != 1) throw std::runtime_error("invalid bytecode stack");
    result.Append(std::move(stack.back()));
  }
  return result;
}

}  // namespace tinylamb
