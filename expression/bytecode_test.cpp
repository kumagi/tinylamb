/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "gtest/gtest.h"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(BytecodeTest, Compile_WithArithmeticAndPredicate_GeneratesTypedBytecodeAndEvaluatesBatch) {
  const Schema schema("input", {Column("id", ValueType::kInt64),
                                Column("price", ValueType::kDouble),
                                Column("date", ValueType::kDate)});
  DataChunk input(schema);
  input.Append(Row({Value(2), Value(1.5), Value::Date("1995-01-01")}));
  input.Append(Row({Value(5), Value(2.0), Value::Date("1996-01-01")}));
  input.Append(Row({Value(), Value(3.0), Value::Date("1997-01-01")}));
  Expression arithmetic = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("id"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2))),
      BinaryOperation::kMultiply, ConstantValueExp(Value(3)));
  Expression predicate = BinaryExpressionExp(
      arithmetic, BinaryOperation::kGreaterThan, ConstantValueExp(Value(10)));

  auto program = BytecodeCompiler::Compile(predicate, schema);
  if (!program.has_value()) {
    GTEST_FAIL() << "compilation failed";
    return;
  }
  const BytecodeProgram& compiled = *program;
  const ColumnVector output = compiled.EvaluateBatch(input);

  EXPECT_TRUE(std::ranges::any_of(
      compiled.Instructions(), [](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kBinaryInt64;
      }));
  EXPECT_EQ(output.ValueAt(0), Value(true));
  EXPECT_EQ(output.ValueAt(1), Value(true));
  EXPECT_TRUE(output.ValueAt(2).IsNull());
}

TEST(BytecodeTest, Compile_WithConstantExpressions_FoldsToOneLoadInstruction) {
  const Schema schema;
  Expression constants = BinaryExpressionExp(
      ConstantValueExp(Value(2)), BinaryOperation::kAdd,
      BinaryExpressionExp(ConstantValueExp(Value(3)),
                          BinaryOperation::kMultiply,
                          ConstantValueExp(Value(4))));

  auto program = BytecodeCompiler::Compile(constants, schema);
  if (!program.has_value()) {
    GTEST_FAIL() << "compilation failed";
    return;
  }
  const BytecodeProgram& folded = *program;

  EXPECT_EQ(folded.Instructions().size(), 1U);
  EXPECT_EQ(folded.Constants(), std::vector<Value>{Value(14)});
}

TEST(BytecodeTest, Compile_WithDateComparison_UsesTypedDateOpcode) {
  const Schema schema("dates", {Column("date", ValueType::kDate)});
  DataChunk input(schema);
  input.Append(Row({Value::Date("1995-06-01")}));
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp("date"), BinaryOperation::kLessThan,
      ConstantValueExp(Value::Date("1996-01-01")));

  auto program = BytecodeCompiler::Compile(predicate, schema);
  if (!program.has_value()) {
    GTEST_FAIL() << "compilation failed";
    return;
  }
  const BytecodeProgram& date_program = *program;
  const ColumnVector output = date_program.EvaluateBatch(input);

  ASSERT_EQ(date_program.Instructions().size(), 3U);
  EXPECT_EQ(date_program.Instructions().back().opcode, BytecodeOp::kBinaryDate);
  EXPECT_EQ(output.ValueAt(0), Value(true));
}

TEST(BytecodeTest, EvaluateBatch_WithInt64Comparisons_MatchesAstAndJitSemantics) {
  constexpr int64_t kConstant = 17;
  const Schema schema("input", {Column("x", ValueType::kInt64)});
  DataChunk chunk(schema);
  const std::vector<std::optional<int64_t>> samples{
      -2048, 0, 16, 17, 18, 1024, std::nullopt};
  for (const std::optional<int64_t>& sample : samples) {
    chunk.Append(Row({sample ? Value(*sample) : Value()}));
  }

  const std::vector<BinaryOperation> operations{
      BinaryOperation::kEquals,       BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,     BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan,  BinaryOperation::kGreaterThanEquals,
  };

  for (const BinaryOperation op : operations) {
    const Expression expression = BinaryExpressionExp(
        ColumnValueExp("x"), op, ConstantValueExp(Value(kConstant)));
    const auto program = BytecodeCompiler::Compile(expression, schema);
    if (!program.has_value()) {
      GTEST_FAIL() << "compilation failed: " << static_cast<int>(op);
      return;
    }
    const BytecodeProgram& op_program = *program;
    ASSERT_EQ(op_program.Instructions().size(), 3U);
    const ColumnVector bytecode_out = op_program.EvaluateBatch(chunk);

    auto jit = JitInt64Kernels::CompileFilter(op);
    if (!jit.has_value()) {
      GTEST_FAIL() << "jit compile failed: " << static_cast<int>(op);
      return;
    }
    std::vector<int64_t> column_values;
    column_values.reserve(chunk.Size());
    for (size_t row = 0; row < chunk.Size(); ++row) {
      const Value ast = expression->Evaluate(chunk.RowAt(row), schema);
      const Value bytecode = bytecode_out.ValueAt(row);
      EXPECT_EQ(ast, bytecode) << row << ' ' << static_cast<int>(op);
      if (!ast.IsNull()) {
        column_values.push_back(chunk.RowAt(row)[0].value.int_value);
      }
    }

    if (!column_values.empty()) {
      std::vector<uint8_t> jit_out(column_values.size());
      jit->Filter(column_values.data(), jit_out.data(), jit_out.size(),
                  kConstant);
      size_t jit_row = 0;
      for (size_t row = 0; row < chunk.Size(); ++row) {
        const Value bytecode = bytecode_out.ValueAt(row);
        if (bytecode.IsNull()) {
          continue;
        }
        EXPECT_EQ(bytecode, Value(jit_out[jit_row++] != 0))
            << row << ' ' << static_cast<int>(op);
      }
      EXPECT_EQ(jit_row, column_values.size());
    }
  }
}

TEST(BytecodeTest, D7_CompilesAndOrToShortCircuitJumps) {
  // D7 (docs/design.md): AND must emit JumpIfFalse and OR must emit
  // JumpIfTrue before the right operand, mirroring the AST evaluator.
  const Schema schema("input", {Column("i", ValueType::kInt64),
                                Column("j", ValueType::kInt64)});
  Expression lhs = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kNotEquals,
      ConstantValueExp(Value(0)));
  Expression rhs = BinaryExpressionExp(
      ColumnValueExp("j"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(1)));
  Expression conj = BinaryExpressionExp(lhs, BinaryOperation::kAnd, rhs);
  auto program = BytecodeCompiler::Compile(conj, schema);
  ASSERT_TRUE(program.has_value());
  EXPECT_TRUE(std::ranges::any_of(
      program->Instructions(), [](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kJumpIfFalse;
      }));

  Expression disj = BinaryExpressionExp(lhs, BinaryOperation::kOr, rhs);
  auto or_program = BytecodeCompiler::Compile(disj, schema);
  ASSERT_TRUE(or_program.has_value());
  EXPECT_TRUE(std::ranges::any_of(
      or_program->Instructions(), [](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kJumpIfTrue;
      }));
  // Every jump target must land inside the program (stack-depth invariant
  // guard from D7: no dangling labels).
  for (const BytecodeInstruction& instruction : or_program->Instructions()) {
    if (instruction.opcode == BytecodeOp::kJumpIfTrue ||
        instruction.opcode == BytecodeOp::kJumpIfFalse) {
      const long long target =
          static_cast<long long>(&instruction - or_program->Instructions().data()) +
          instruction.jump_target;
      EXPECT_GE(target, 0);
      EXPECT_LE(target, static_cast<long long>(or_program->Instructions().size()));
    }
  }
}

TEST(BytecodeTest, D7_RightHandSideErrorsAreSuppressedByShortCircuit) {
  // `i <> 0 AND 10 / j > 1`: rows where i = 0 must NOT evaluate 10 / j, so
  // j = 0 divides by zero ONLY on rows whose left side is TRUE or NULL
  // (identical to the AST semantics).
  const Schema schema("input", {Column("i", ValueType::kInt64),
                                Column("j", ValueType::kInt64)});
  Expression guard = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kNotEquals,
      ConstantValueExp(Value(0)));
  Expression division = BinaryExpressionExp(
      ConstantValueExp(Value(10)), BinaryOperation::kDivide,
      ColumnValueExp("j"));
  Expression conj = BinaryExpressionExp(
      guard, BinaryOperation::kAnd,
      BinaryExpressionExp(division, BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1))));
  auto program = BytecodeCompiler::Compile(conj, schema);
  ASSERT_TRUE(program.has_value());

  {
    DataChunk safe_rows(schema);
    safe_rows.Append(Row({Value(0), Value(0)}));  // FALSE AND (10/0) -> FALSE
    const ColumnVector output = program->EvaluateBatch(safe_rows);
    EXPECT_EQ(output.ValueAt(0), Value(false));
  }
  {
    DataChunk throwing_rows(schema);
    throwing_rows.Append(Row({Value(1), Value(0)}));  // TRUE AND (10/0) -> error
    EXPECT_THROW(static_cast<void>(program->EvaluateBatch(throwing_rows)),
                  std::exception);
  }
}

}  // namespace tinylamb
