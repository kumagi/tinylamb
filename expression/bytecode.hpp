/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_BYTECODE_HPP
#define TINYLAMB_EXPRESSION_BYTECODE_HPP

#include <cstddef>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

#include "common/status_or.hpp"
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
  // EXTRACT(year|month|day FROM date): pops one DATE/VARCHAR value and pushes
  // the extracted part as INT64.  `operand` selects the part (0 = year,
  // 1 = month, 2 = day).  The kernel reproduces the AST's
  // extract_{year,month,day} evaluator (string-slice + stoll) exactly,
  // including its error conditions, so differential coverage holds.
  kExtractDatePart,
  // Discards the stack top.  The searched-CASE lowering uses it to drop a
  // WHEN condition that was already peeked by its conditional jump (the
  // jump decides without popping), keeping the stack depth invariant across
  // every branch path.
  kPop,
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
  [[nodiscard]] StatusOr<ColumnVector> TryEvaluateBatch(
      const DataChunk& input) const;
  // EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp); library
  // callers use TryEvaluateBatch.
  [[nodiscard]] ColumnVector EvaluateBatch(const DataChunk& input) const;
  // Single-row evaluation for use in row-by-row filter paths (e.g.
  // MatchScanFilter).  Returns the result of evaluating the compiled
  // expression against the given row.
  [[nodiscard]] StatusOr<Value> TryEvaluateRow(const Row& row) const;
  // EXC-SHIM: deprecated throwing wrapper; library callers use TryEvaluateRow.
  [[nodiscard]] Value EvaluateRow(const Row& row) const;

  // Boxing-free interpreter result: kDone fills *out, kError fills *error,
  // kUnsupported means the program shape or an operand needs the canonical
  // interpreter (the caller retries with TryEvaluateRow; nothing was
  // consumed).
  enum class FastEval { kDone, kUnsupported, kError };
  // Evaluates numeric programs (columns/constants, int64+double arithmetic,
  // comparisons, unary, EXTRACT date parts) straight on the row cells without
  // constructing intermediate Values.  Semantics mirror the canonical
  // interpreter instruction-for-instruction; differential_test compares both
  // paths.  Only valid after a successful Compile (the shape flag is computed
  // there; hand-built programs report kUnsupported).
  [[nodiscard]] FastEval TryEvaluateRowFast(const Row& row, Value* out,
                                            Status* error) const;
  // True when every instruction is in the fast interpreter's supported set
  // and the constants it loads are numeric (the Compile-time shape scan).
  [[nodiscard]] bool FastInterpretable() const { return fast_interpretable_; }
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
  bool AddConstant(Value value, uint16_t* offset) {
    if (constants_.size() >= std::numeric_limits<uint16_t>::max()) {
      return false;
    }
    constants_.push_back(std::move(value));
    *offset = static_cast<uint16_t>(constants_.size() - 1);
    return true;
  }
  void SetResultType(ValueType type) { result_type_ = type; }
  // Compile-time shape scan: marks whether TryEvaluateRowFast can take this
  // program.  Only the compiler may call it (hand-built programs stay
  // canonical-only, which keeps the flag an under-approximation).
  void SetFastInterpretable(bool fast) { fast_interpretable_ = fast; }

 private:
  friend class BytecodeCompiler;
  std::vector<BytecodeInstruction> instructions_;
  std::vector<Value> constants_;
  ValueType result_type_{ValueType::kNull};
  bool fast_interpretable_{false};
};

class BytecodeCompiler {
 public:
  // kValue: select items / sort keys — a rewrite that turns a NULL result
  // into FALSE (inner_join_not_null_inference) must not apply there.
  // kFilter: WHERE/ON and other truthiness-only contexts, where the same
  // rewrite is sound and kept enabled.
  enum class Context { kValue, kFilter };
  [[nodiscard]] static std::optional<BytecodeProgram> Compile(
      const Expression& expression, const Schema& schema,
      Context context = Context::kValue);
};

// Differential-testing hook: false when TINYLAMB_DISABLE_BYTECODE is set in
// the environment. Compile call sites gate on this so the same query can be
// executed through the AST evaluation path (the semantic reference) and
// compared against the bytecode/JIT fast path.
[[nodiscard]] bool BytecodeEnabled();

// Row evaluation through the boxing-free interpreter with a canonical
// fallback: fast-shaped programs (columns/constants/int64+double arithmetic)
// skip intermediate Value construction; every other shape runs
// BytecodeProgram::TryEvaluateRow.  Semantics are identical on both legs
// (differential_test pins this).
[[nodiscard]] StatusOr<Value> TryEvaluateRowAuto(const BytecodeProgram& program,
                                                 const Row& row);

}  // namespace tinylamb
#endif
