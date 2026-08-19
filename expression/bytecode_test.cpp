/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <algorithm>
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "gtest/gtest.h"
#include "type/date.hpp"

namespace tinylamb {

TEST(BytecodeTest, CompilesTypedInstructionsAndEvaluatesBatch) {
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
  ASSERT_TRUE(program);
  EXPECT_TRUE(std::ranges::any_of(
      program->Instructions(), [](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kBinaryInt64;
      }));

  const ColumnVector output = program->EvaluateBatch(input);
  EXPECT_EQ(output.ValueAt(0), Value(true));
  EXPECT_EQ(output.ValueAt(1), Value(true));
  EXPECT_TRUE(output.ValueAt(2).IsNull());
}

TEST(BytecodeTest, ConstantFoldingReducesProgramToOneLoad) {
  const Schema schema;
  Expression constants = BinaryExpressionExp(
      ConstantValueExp(Value(2)), BinaryOperation::kAdd,
      BinaryExpressionExp(ConstantValueExp(Value(3)),
                          BinaryOperation::kMultiply,
                          ConstantValueExp(Value(4))));
  auto program = BytecodeCompiler::Compile(constants, schema);
  ASSERT_TRUE(program);
  EXPECT_EQ(program->Instructions().size(), 1U);
  EXPECT_EQ(program->Constants(), std::vector<Value>{Value(14)});
}

TEST(BytecodeTest, DateComparisonUsesTypedDateOpcode) {
  const Schema schema("dates", {Column("date", ValueType::kDate)});
  DataChunk input(schema);
  input.Append(Row({Value::Date("1995-06-01")}));
  Expression predicate = BinaryExpressionExp(
      ColumnValueExp("date"), BinaryOperation::kLessThan,
      ConstantValueExp(Value::Date("1996-01-01")));
  auto program = BytecodeCompiler::Compile(predicate, schema);
  ASSERT_TRUE(program);
  ASSERT_EQ(program->Instructions().size(), 3U);
  EXPECT_EQ(program->Instructions().back().opcode, BytecodeOp::kBinaryDate);
  EXPECT_EQ(program->EvaluateBatch(input).ValueAt(0), Value(true));
}

}  // namespace tinylamb
