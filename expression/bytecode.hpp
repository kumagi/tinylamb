/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_BYTECODE_HPP
#define TINYLAMB_EXPRESSION_BYTECODE_HPP

#include <cstddef>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

#include "executor/data_chunk.hpp"
#include "expression/expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {

enum class BytecodeOp : uint8_t {
  kLoadColumn,
  kLoadConstant,
  kBinaryInt64,
  kBinaryDouble,
  kBinaryVarchar,
  kBinaryDate,
  kUnaryInt64,
  kUnaryDouble,
};

struct BytecodeInstruction {
  BytecodeOp opcode;
  uint16_t operand{0};
  BinaryOperation binary{BinaryOperation::kAdd};
  UnaryOperation unary{UnaryOperation::kNot};
};

class BytecodeProgram {
 public:
  [[nodiscard]] ColumnVector EvaluateBatch(const DataChunk& input) const;
  [[nodiscard]] const std::vector<BytecodeInstruction>& Instructions() const {
    return instructions_;
  }
  [[nodiscard]] const std::vector<Value>& Constants() const {
    return constants_;
  }
  [[nodiscard]] ValueType ResultType() const { return result_type_; }
  void AddInstruction(BytecodeInstruction instruction) {
    instructions_.push_back(instruction);
  }
  [[nodiscard]] uint16_t AddConstant(Value value) {
    if (constants_.size() >= std::numeric_limits<uint16_t>::max()) {
      throw std::runtime_error("too many bytecode constants");
    }
    constants_.push_back(std::move(value));
    return static_cast<uint16_t>(constants_.size() - 1);
  }
  void SetResultType(ValueType type) { result_type_ = type; }

 private:
  friend class BytecodeCompiler;
  std::vector<BytecodeInstruction> instructions_;
  std::vector<Value> constants_;
  ValueType result_type_{ValueType::kNull};
};

class BytecodeCompiler {
 public:
  [[nodiscard]] static std::optional<BytecodeProgram> Compile(
      const Expression& expression, const Schema& schema);
};

}  // namespace tinylamb
#endif
