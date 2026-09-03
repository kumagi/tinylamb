/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <cassert>
#include <cstdint>
#include <optional>
#include <exception>
#include <cstddef>
#include <stdexcept>
#include <limits>
#include <vector>
#include <utility>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "type/value_type.hpp"
#include "type/type.hpp"
#include "type/schema.hpp"

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
    case ValueType::kArray:
    case ValueType::kNull:
      throw std::runtime_error("untyped bytecode operand");
  }
  throw std::runtime_error("untyped bytecode operand");
}

bool CompileNode(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema,
    BytecodeProgram* program) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const int offset =
          schema.Offset(expression->AsColumnValue().GetColumnName());
      if (offset < 0 ||
          std::cmp_greater(offset,
                           std::numeric_limits<uint16_t>::max())) {
        return false;
      }
      program->AddInstruction(
          {.opcode=BytecodeOp::kLoadColumn, .operand=static_cast<uint16_t>(offset)});
      return true;
    }
    case TypeTag::kConstantValue: {
      const uint16_t offset =
          program->AddConstant(expression->AsConstantValue().GetValue());
      program->AddInstruction({.opcode=BytecodeOp::kLoadConstant, .operand=offset});
      return true;
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      // D7 (docs/design.md): AND/OR compile to short-circuit control flow so
      // the VM skips the right operand exactly when the AST does, keeping the
      // three-valued truth table AND the error-occurrence conditions
      // identical.  Layout (AND):
      //     <left>
      //     JumpIfFalse -> end     (left==FALSE: result FALSE, skip right)
      //     <right>
      //     Binary AND             (left TRUE/NULL: merge three-valued)
      //   end:
      // OR uses JumpIfTrue symmetrically.  The jump peeks the stack (does not
      // pop), so every path reaches `end` with exactly one value.
      if (binary.Op() == BinaryOperation::kAnd ||
          binary.Op() == BinaryOperation::kOr) {
        if (!CompileNode(binary.Left(), schema, program)) { return false;
}
        const size_t jump_index = program->Size();
        program->AddInstruction({.opcode = binary.Op() == BinaryOperation::kAnd
                                              ? BytecodeOp::kJumpIfFalse
                                              : BytecodeOp::kJumpIfTrue});
        if (!CompileNode(binary.Right(), schema, program)) { return false;
}
        program->AddInstruction(
            {.opcode=BytecodeOp::kBinaryInt64, .operand=0, .binary=binary.Op()});
        // Patch: jump lands just past the merge instruction.
        program->SetJumpTarget(jump_index,
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(jump_index));
        return true;
      }
      if (!CompileNode(binary.Left(), schema, program) ||
          !CompileNode(binary.Right(), schema, program)) {
        return false;
      }
      const ValueType left_type = ValueTypeFor(binary.Left()->ResultType(schema));
      const ValueType right_type = ValueTypeFor(binary.Right()->ResultType(schema));
      if (binary.Op() == BinaryOperation::kLike ||
          binary.Op() == BinaryOperation::kNotLike) {
        if (left_type != ValueType::kVarChar || right_type != ValueType::kVarChar) {
          return false;
        }
      }
      ValueType operand_type = left_type;
      if (operand_type == ValueType::kDouble || right_type == ValueType::kDouble) {
        operand_type = ValueType::kDouble;
      }
      program->AddInstruction(
          {.opcode=BinaryOpcode(operand_type), .operand=0, .binary=binary.Op()});
      return true;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expression->AsUnaryExpression();
      if (!CompileNode(unary.Child(), schema, program)) { return false;
}
      const ValueType operand_type = ValueTypeFor(unary.Child()->ResultType(schema));
      if (operand_type != ValueType::kInt64 &&
          operand_type != ValueType::kDouble) {
        return false;
      }
      program->AddInstruction(
          {.opcode=operand_type == ValueType::kDouble ? BytecodeOp::kUnaryDouble
                                              : BytecodeOp::kUnaryInt64,
           .operand=0, .binary=BinaryOperation::kAdd, .unary=unary.Op()});
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
    if (!CompileNode(folded, schema, &program)) { return std::nullopt;
}
    const ValueType result_type = ValueTypeFor(folded->ResultType(schema));
    if (result_type == ValueType::kNull) { return std::nullopt;
}
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
    // D7: a program counter (not a range-for) so the short-circuit jumps can
    // skip the right operand; jump targets are relative to the jump itself.
    for (size_t pc = 0; pc < instructions_.size(); ++pc) {
      const BytecodeInstruction& instruction = instructions_[pc];
      switch (instruction.opcode) {
        case BytecodeOp::kLoadColumn:
          stack.push_back(input.ColumnAt(instruction.operand).ValueAt(row));
          break;
        case BytecodeOp::kLoadConstant:
          stack.push_back(constants_[instruction.operand]);
          break;
        case BytecodeOp::kJumpIfFalse:
        case BytecodeOp::kJumpIfTrue: {
          assert(!stack.empty());
          const Value& top = stack.back();
          const bool decides =
              instruction.opcode == BytecodeOp::kJumpIfFalse
                  ? (!top.IsNull() && !top.Truthy())
                  : (!top.IsNull() && top.Truthy());
          if (decides) {
            pc += static_cast<size_t>(instruction.jump_target);
            --pc;  // the for-loop ++pc lands on the target.
          }
          break;
        }
        case BytecodeOp::kJump:
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
          break;
        case BytecodeOp::kBinaryInt64:
        case BytecodeOp::kBinaryDouble:
        case BytecodeOp::kBinaryVarchar:
        case BytecodeOp::kBinaryDate: {
          // Only compiler-generated programs reach here; an empty stack means
          // a malformed program.
          assert(!stack.empty());
          Value right = std::move(stack.back());
          stack.pop_back();
          assert(!stack.empty());
          Value left = std::move(stack.back());
          stack.pop_back();
          stack.push_back(EvaluateBinary(instruction.binary, left, right));
          break;
        }
        case BytecodeOp::kUnaryInt64:
        case BytecodeOp::kUnaryDouble: {
          assert(!stack.empty());
          Value child = std::move(stack.back());
          stack.pop_back();
          stack.push_back(EvaluateUnary(instruction.unary, child));
          break;
        }
      }
    }
    if (stack.size() != 1) { throw std::runtime_error("invalid bytecode stack");
}
    result.Append(stack.back());
  }
  return result;
}

Value BytecodeProgram::EvaluateRow(const Row& row) const {
  std::vector<Value> stack;
  stack.reserve(instructions_.size());
  for (size_t pc = 0; pc < instructions_.size(); ++pc) {
    const BytecodeInstruction& instruction = instructions_[pc];
    switch (instruction.opcode) {
      case BytecodeOp::kLoadColumn:
        stack.push_back(row[instruction.operand]);
        break;
      case BytecodeOp::kLoadConstant:
        stack.push_back(constants_[instruction.operand]);
        break;
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        assert(!stack.empty());
        const Value& top = stack.back();
        const bool decides =
            instruction.opcode == BytecodeOp::kJumpIfFalse
                ? (!top.IsNull() && !top.Truthy())
                : (!top.IsNull() && top.Truthy());
        if (decides) {
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        break;
      }
      case BytecodeOp::kJump:
        pc += static_cast<size_t>(instruction.jump_target);
        --pc;
        break;
      case BytecodeOp::kBinaryInt64:
      case BytecodeOp::kBinaryDouble:
      case BytecodeOp::kBinaryVarchar:
      case BytecodeOp::kBinaryDate: {
        assert(!stack.empty());
        Value right = std::move(stack.back());
        stack.pop_back();
        assert(!stack.empty());
        Value left = std::move(stack.back());
        stack.pop_back();
        stack.push_back(EvaluateBinary(instruction.binary, left, right));
        break;
      }
      case BytecodeOp::kUnaryInt64:
      case BytecodeOp::kUnaryDouble: {
        assert(!stack.empty());
        Value child = std::move(stack.back());
        stack.pop_back();
        stack.push_back(EvaluateUnary(instruction.unary, child));
        break;
      }
    }
  }
  if (stack.size() != 1) { throw std::runtime_error("invalid bytecode stack"); }
  return std::move(stack.back());
}

}  // namespace tinylamb
