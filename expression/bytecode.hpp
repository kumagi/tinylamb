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
#include "type/row.hpp"
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
  // D7 (docs/design.md): short-circuit control flow.  The jump target is a
  // relative offset from the jump instruction itself (0 == no-op).  These
  // let AND/OR skip the right-hand side exactly like the AST evaluator and
  // keep the three-valued logic table identical across AST / VM / JIT.
  kJumpIfFalse,
  kJumpIfTrue,
  kJump,
};

struct BytecodeInstruction {
  BytecodeOp opcode;
  uint16_t operand{0};
  BinaryOperation binary{BinaryOperation::kAdd};
  UnaryOperation unary{UnaryOperation::kNot};
  // Relative jump target (D7): positive = forward, negative = backward.
  // Stored as int32 in two halves to avoid changing the struct layout
  // elsewhere; only the control-flow ops read it.
  int32_t jump_target{0};
};

class BytecodeProgram {
 public:
  [[nodiscard]] ColumnVector EvaluateBatch(const DataChunk& input) const;
  // Single-row evaluation for use in row-by-row filter paths (e.g.
  // MatchScanFilter).  Returns the result of evaluating the compiled
  // expression against the given row.
  [[nodiscard]] Value EvaluateRow(const Row& row) const;
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
  // D7: the compiler emits a conditional jump before knowing the target,
  // then patches it once the short-circuit label is reached.
  [[nodiscard]] size_t Size() const { return instructions_.size(); }
  void SetJumpTarget(size_t index, int32_t relative_offset) {
    instructions_[index].jump_target = relative_offset;
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
