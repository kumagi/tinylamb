/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/parameter_slot.hpp"
#include "gtest/gtest.h"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(BytecodeTest, AuditBatchProjectionPreservesUnsignedValuesAfterNull) {
  Column column("u", ValueType::kInt64);
  column.SetUnsigned(true);
  const Schema schema("input", {column});
  DataChunk input(schema);
  input.Append(Row({Value()}));
  input.Append(Row({Value(-1).WithUnsigned()}));
  const auto expression = ColumnValueExp("u");
  const auto program = BytecodeCompiler::Compile(expression, schema);
  ASSERT_TRUE(program.has_value());
  if (!program) {
    return;
  }
  const auto output = program->TryEvaluateBatch(input);
  ASSERT_TRUE(output.HasValue());
  EXPECT_TRUE(output.Value().ValueAt(0).IsNull());
  EXPECT_TRUE(output.Value().ValueAt(1).IsUnsigned());
  EXPECT_EQ(output.Value().ValueAt(1), Value(-1).WithUnsigned());

  // The columnar fast path must decline unsigned operands (P668): FastSlot
  // cannot carry the unsigned tag, so a regression that starts taking the
  // fast path here would silently hand back signed INT64 bits. Mirror the
  // batch above with a non-uniform column view (NULL followed by the
  // unsigned value) and pin that it reports kUnsupported with an untouched
  // sink instead of producing a lossy result.
  const std::vector<Value> values{Value(), Value(-1).WithUnsigned()};
  const BytecodeColumnView view{.type = ValueType::kInt64,
                                .is_unsigned = true,
                                .is_uniform = false,
                                .fallback = values.data()};
  const std::vector<BytecodeColumnView> cols{view};
  std::vector<int64_t> ints;
  BytecodeColumnSink sink;
  sink.ints = &ints;
  Status error;
  EXPECT_EQ(program->TryEvaluateColumnarBatch(cols, nullptr, values.size(),
                                              &sink, &error),
            BytecodeProgram::FastEval::kUnsupported);
  EXPECT_TRUE(ints.empty());
}

// T4800: FastSlot cannot carry Value's unsigned tag, so an unsigned input
// must never take the fast VM's signed semantics. The loader rejects
// unsigned values (kUnsupported -> canonical VM / AST) and the pure-
// projection fast path hands back the raw Value keeping the tag.
TEST(BytecodeTest, UnsignedInputsNeverTakeSignedFastMath) {
  const Schema schema("input", {Column("u", ValueType::kInt64)});
  const Row row({Value(-1).WithUnsigned()});
  const std::vector<std::pair<std::string, Expression>> cases{
      {"u", ColumnValueExp("u")},
      {"u + 0", BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kAdd,
                                    ConstantValueExp(Value(int64_t{0})))},
      {"u > 1",
       BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kGreaterThan,
                           ConstantValueExp(Value(int64_t{1})))},
      {"-u", UnaryExpressionExp(ColumnValueExp("u"), UnaryOperation::kMinus)},
  };
  for (const auto& [name, expression] : cases) {
    const auto program = BytecodeCompiler::Compile(expression, schema);
    ASSERT_TRUE(program.has_value()) << name;
    const auto expected = expression->TryEvaluate(row, schema);
    const auto actual = program->TryEvaluateRow(
        row);  // NOLINT(bugprone-unchecked-optional-access)
    ASSERT_EQ(actual.HasValue(), expected.HasValue()) << name;
    if (expected.HasValue()) {
      EXPECT_EQ(actual.Value().IsNull(), expected.Value().IsNull()) << name;
      EXPECT_EQ(actual.Value().IsUnsigned(), expected.Value().IsUnsigned())
          << name;
      EXPECT_EQ(actual.Value(), expected.Value()) << name;
    } else {
      EXPECT_EQ(actual.GetStatus(), expected.GetStatus()) << name;
    }
    Value fast_value;
    Status fast_error;
    const auto fast = program->TryEvaluateRowFast(
        row, &fast_value,
        &fast_error);  // NOLINT(bugprone-unchecked-optional-access)
    if (fast == BytecodeProgram::FastEval::kDone && expected.HasValue()) {
      // A kDone result over an unsigned operand must still be unsigned;
      // signed bits without the tag is exactly the T4800 divergence.
      EXPECT_TRUE(fast_value.IsUnsigned() || fast_value.IsNull()) << name;
    }
  }
}

TEST(BytecodeTest, ByteLengthMatchesAstForStringsAndDynamicDomains) {
  // GoogleSQL: BYTE_LENGTH/OCTET_LENGTH count raw bytes (kByteLength) while
  // LENGTH/CHAR_LENGTH/CHARACTER_LENGTH count Unicode code points
  // (kCharLength) -- LENGTH is a CHAR_LENGTH synonym, not a byte count. The
  // opcode pin below keeps the two families distinct; a multibyte value in
  // `values` (e.g. "é東京") makes them disagree, so both are exercised here.
  const Schema schema("input", {Column("v", ValueType::kVarChar)});
  const std::vector<Value> values{Value(),
                                  Value(""),
                                  Value("é東京"),
                                  Value(std::string("a\0b", 3)),
                                  Value("collated").WithCollation(2),
                                  Value(-123),
                                  Value(-1).WithUnsigned(),
                                  Value(1.25),
                                  Value::DateFromDays(-10),
                                  Value::Array({Value(1), Value()}, "INT64")};
  for (const std::string name :
       {"byte_length", "octet_length", "length", "char_length"}) {
    const auto expression = FunctionCallExp(name, {ColumnValueExp("v")});
    const auto program = BytecodeCompiler::CompileRowCached(expression, schema);
    ASSERT_TRUE(program);
    if (!program) {
      return;
    }
    const BytecodeOp expected_opcode = name == "length" || name == "char_length"
                                           ? BytecodeOp::kCharLength
                                           : BytecodeOp::kByteLength;
    ASSERT_EQ(program->Instructions().back().opcode, expected_opcode);
    ASSERT_TRUE(ScalarExpressionCacheKey({expression}, schema));
    DataChunk batch(schema);
    for (const auto& value : values) {
      const Row row({value});
      const auto expected = expression->TryEvaluate(row, schema);
      ASSERT_TRUE(expected.HasValue());
      const auto actual = program->TryEvaluateRow(row);
      const auto automatic = TryEvaluateRowAuto(*program, row);
      ASSERT_TRUE(actual.HasValue());
      ASSERT_TRUE(automatic.HasValue());
      EXPECT_EQ(actual.Value(), expected.Value());
      EXPECT_EQ(automatic.Value(), expected.Value());
      if (value.IsNull() || value.type == ValueType::kVarChar) {
        batch.Append(row);
      }
    }
    const auto result = program->TryEvaluateBatch(batch);
    ASSERT_TRUE(result.HasValue());
    for (size_t i = 0; i < batch.Size(); ++i) {
      const Row row = batch.RowAt(i);
      EXPECT_EQ(result.Value().ValueAt(i), expression->Evaluate(row, schema));
    }
    EXPECT_FALSE(BytecodeCompiler::Compile(FunctionCallExp(name, {}), schema));
  }
}

TEST(BytecodeTest, DirectColumnAndNullTestsKeepEveryValueDomain) {
  const std::vector<Value> values{
      Value(),
      Value(0),
      Value(1),
      Value(-1).WithUnsigned(),
      Value(-0.0),
      Value(std::numeric_limits<double>::quiet_NaN()),
      Value::DateFromDays(-10),
      Value("text").WithCollation(2),
      Value::Array({Value(1), Value()}, "INT64")};
  for (const auto& value : values) {
    // Derived rows can carry a different runtime domain from their schema.
    const Schema schema("input", {Column("v", ValueType::kInt64)});
    const Row row({value});
    const auto column = ColumnValueExp("v");
    const auto is_null = UnaryExpressionExp(column, UnaryOperation::kIsNull);
    const auto not_null =
        UnaryExpressionExp(column, UnaryOperation::kIsNotNull);
    for (const auto& expression :
         {column, is_null, not_null,
          UnaryExpressionExp(is_null, UnaryOperation::kNot)}) {
      auto program = BytecodeCompiler::CompileRowCached(expression, schema);
      ASSERT_TRUE(program);
      if (!program) {
        return;
      }
      auto expected = expression->TryEvaluate(row, schema);
      ASSERT_TRUE(expected.HasValue());
      Value actual;
      Status error;
      EXPECT_EQ(program->TryEvaluateRowFast(row, &actual, &error),
                BytecodeProgram::FastEval::kDone);
      EXPECT_EQ(actual.type, expected.Value().type);
      EXPECT_EQ(actual.IsUnsigned(), expected.Value().IsUnsigned());
      EXPECT_EQ(actual.Collation(), expected.Value().Collation());
      if (actual.type == ValueType::kDouble &&
          std::isnan(expected.Value().value.double_value)) {
        EXPECT_TRUE(std::isnan(actual.value.double_value));
      } else {
        EXPECT_EQ(actual, expected.Value());
      }
      EXPECT_EQ(program->TryEvaluateRowFast(Row(), &actual, &error),
                BytecodeProgram::FastEval::kUnsupported);
    }
    // Keep the explicit three-instruction spelling covered even when the
    // expression rewriter folds NOT(IS NULL) to IS NOT NULL.
    BytecodeProgram program;
    program.AddInstruction({.opcode = BytecodeOp::kLoadColumn, .operand = 0});
    program.AddInstruction(
        {.opcode = BytecodeOp::kUnaryInt64, .unary = UnaryOperation::kIsNull});
    program.AddInstruction(
        {.opcode = BytecodeOp::kUnaryInt64, .unary = UnaryOperation::kNot});
    program.SetFastInterpretable(true);
    Value actual;
    Status error;
    EXPECT_EQ(program.TryEvaluateRowFast(row, &actual, &error),
              BytecodeProgram::FastEval::kDone);
    EXPECT_EQ(actual, Value(static_cast<int64_t>(!value.IsNull())));
  }
}

TEST(BytecodeTest, CoalescePreservesShortCircuitAndDynamicValueTypes) {
  const Schema schema("coalesce", {Column("i", ValueType::kInt64),
                                   Column("d", ValueType::kDouble)});
  const auto throwing =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kDivide,
                          ColumnValueExp("i"));
  const std::vector<Expression> expressions{
      FunctionCallExp("coalesce",
                      {ColumnValueExp("d"), ConstantValueExp(Value(0))}),
      FunctionCallExp("ifnull",
                      {ColumnValueExp("d"), ConstantValueExp(Value(0))}),
      FunctionCallExp("coalesce", {ColumnValueExp("d"), throwing}),
      FunctionCallExp("coalesce",
                      {ColumnValueExp("i"), ConstantValueExp(Value(7))})};
  for (const auto& expression : expressions) {
    const auto program = BytecodeCompiler::CompileRowCached(expression, schema);
    ASSERT_TRUE(program);
    if (!program) {
      return;
    }
    for (const auto& row :
         {Row({Value(0), Value(3.5)}), Row({Value(0), Value()}),
          Row({Value(2), Value()}), Row({Value(), Value()})}) {
      const auto expected = expression->TryEvaluate(row, schema);
      const auto canonical = program->TryEvaluateRow(row);
      const auto fast = TryEvaluateRowAuto(*program, row);
      ASSERT_EQ(canonical.HasValue(), expected.HasValue());
      ASSERT_EQ(fast.HasValue(), expected.HasValue());
      if (expected.HasValue()) {
        EXPECT_EQ(canonical.Value().type, expected.Value().type);
        EXPECT_EQ(fast.Value().type, expected.Value().type);
        EXPECT_EQ(canonical.Value(), expected.Value());
        EXPECT_EQ(fast.Value(), expected.Value());
      } else {
        EXPECT_EQ(canonical.GetStatus(), expected.GetStatus());
        EXPECT_EQ(fast.GetStatus(), expected.GetStatus());
      }
    }
  }
  EXPECT_FALSE(BytecodeCompiler::Compile(expressions.front(), schema));
  const auto uniform = BytecodeCompiler::Compile(expressions.back(), schema);
  ASSERT_TRUE(uniform);
  if (!uniform) {
    return;
  }
  DataChunk batch(schema);
  batch.Append(Row({Value(), Value()}));
  batch.Append(Row({Value(8), Value()}));
  const auto result = uniform->TryEvaluateBatch(batch);
  ASSERT_TRUE(result.HasValue());
  EXPECT_EQ(result.Value().ValueAt(0), Value(7));
  EXPECT_EQ(result.Value().ValueAt(1), Value(8));
}

TEST(BytecodeTest, FastNullSafeComparisonKeepsTextAndDateDomains) {
  for (const auto op : {BinaryOperation::kIsDistinctFrom,
                        BinaryOperation::kIsNotDistinctFrom}) {
    for (const ValueType type : {ValueType::kVarChar, ValueType::kDate}) {
      const Schema schema("domains", {Column("a", type), Column("b", type)});
      const auto expression =
          BinaryExpressionExp(ColumnValueExp("a"), op, ColumnValueExp("b"));
      const auto program =
          BytecodeCompiler::CompileRowCached(expression, schema);
      ASSERT_TRUE(program);
      if (!program) {
        return;
      }
      const Value first =
          type == ValueType::kVarChar ? Value("alpha") : Value::DateFromDays(1);
      const Value second =
          type == ValueType::kVarChar ? Value("beta") : Value::DateFromDays(2);
      for (const Row& row :
           {Row({first, first}), Row({first, second}), Row({Value(), first}),
            Row({first, Value()}), Row({Value(), Value()})}) {
        const auto expected = expression->TryEvaluate(row, schema);
        const auto actual = TryEvaluateRowAuto(*program, row);
        ASSERT_TRUE(expected.HasValue());
        ASSERT_TRUE(actual.HasValue());
        EXPECT_EQ(actual.Value(), expected.Value());
      }
    }
    const Schema schema("mixed", {Column("a", ValueType::kDate),
                                  Column("b", ValueType::kInt64)});
    const auto expression =
        BinaryExpressionExp(ColumnValueExp("a"), op, ColumnValueExp("b"));
    const auto program = BytecodeCompiler::CompileRowCached(expression, schema);
    ASSERT_TRUE(program);
    if (!program) {
      return;
    }
    const Row row({Value::DateFromDays(5), Value(5)});
    const auto expected = expression->TryEvaluate(row, schema);
    const auto actual = TryEvaluateRowAuto(*program, row);
    ASSERT_EQ(actual.HasValue(), expected.HasValue());
    if (expected.HasValue()) {
      EXPECT_EQ(actual.Value(), expected.Value());
    } else {
      EXPECT_EQ(actual.GetStatus(), expected.GetStatus());
    }
  }
}

TEST(BytecodeTest, ScalarCacheKeySnapshotsCurrentBindingsAndDeclinesFunctions) {
  const Schema schema("snapshot", {Column("k", ValueType::kInt64)});
  auto slot = std::make_shared<ParameterSlot>(0, Value(1));
  auto expression =
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals, slot);
  const auto first = ScalarExpressionCacheKey({expression}, schema);
  ASSERT_TRUE(first);
  const auto frozen = FreezeScalarBindings(expression);
  slot->Bind(Value(2));
  const auto second = ScalarExpressionCacheKey({expression}, schema);
  ASSERT_TRUE(second);
  EXPECT_NE(first, second);
  EXPECT_EQ(frozen->Evaluate(Row({Value(1)}), schema), Value(true));
  EXPECT_EQ(expression->Evaluate(Row({Value(1)}), schema), Value(false));
  EXPECT_FALSE(ScalarExpressionCacheKey({FunctionCallExp("rand", {})}, schema));
  const auto prepared =
      std::make_shared<const PreparedValues>(std::vector<Value>{Value(9)});
  const Expression deferred =
      std::make_shared<ParameterSlot>(0, ValueType::kInt64, prepared);
  EXPECT_FALSE(ScalarExpressionCacheKey({deferred}, schema));
}

TEST(BytecodeTest, RowCompileCacheKeepsBindingsSchemasAndDynamicCaseTypes) {
  const Schema schema("cache", {Column("i", ValueType::kInt64),
                                Column("d", ValueType::kDouble)});
  auto slot = std::make_shared<ParameterSlot>(0, Value(1));
  const auto predicate = BinaryExpressionExp(
      ColumnValueExp("i"), BinaryOperation::kGreaterThan, slot);
  for (int64_t bound : {1, 10, -3, 1}) {
    slot->Bind(Value(bound));
    for (auto context : {BytecodeCompiler::Context::kValue,
                         BytecodeCompiler::Context::kFilter}) {
      auto program =
          BytecodeCompiler::CompileRowCached(predicate, schema, context);
      ASSERT_TRUE(program);
      if (!program) {
        return;
      }
      auto value = program->TryEvaluateRow(Row({Value(5), Value(2.5)}));
      ASSERT_TRUE(value.HasValue());
      EXPECT_EQ(value.Value(), Value(5 > bound));
    }
  }
  slot->Bind(Value(1));
  const auto conditional = CaseExpressionExp({{predicate, ColumnValueExp("d")}},
                                             ConstantValueExp(Value(0)));
  EXPECT_FALSE(BytecodeCompiler::Compile(conditional, schema));
  for (int repetition = 0; repetition < 2; ++repetition) {
    auto program = BytecodeCompiler::CompileRowCached(conditional, schema);
    ASSERT_TRUE(program);
    if (!program) {
      return;
    }
    EXPECT_EQ(program->ResultType(), ValueType::kNull);
    for (const Row& row :
         {Row({Value(5), Value(2.5)}), Row({Value(0), Value(7.5)}),
          Row({Value(), Value()})}) {
      auto expected = conditional->TryEvaluate(row, schema);
      auto actual = program->TryEvaluateRow(row);
      ASSERT_TRUE(expected.HasValue());
      ASSERT_TRUE(actual.HasValue());
      EXPECT_EQ(actual.Value().type, expected.Value().type);
      EXPECT_EQ(actual.Value(), expected.Value());
    }
  }
  const Schema reordered("cache", {Column("d", ValueType::kDouble),
                                   Column("i", ValueType::kInt64)});
  auto reordered_program =
      BytecodeCompiler::CompileRowCached(predicate, reordered);
  ASSERT_TRUE(reordered_program);
  if (!reordered_program) {
    return;
  }
  EXPECT_EQ(reordered_program->EvaluateRow(Row({Value(50.0), Value(0)})),
            Value(false));
  Value signed_value(int64_t{-1});
  for (const Value& constant :
       {signed_value, signed_value.WithUnsigned(), signed_value}) {
    auto program =
        BytecodeCompiler::CompileRowCached(ConstantValueExp(constant), schema);
    ASSERT_TRUE(program);
    if (!program) {
      return;
    }
    const auto actual = program->EvaluateRow(Row());
    EXPECT_EQ(actual.IsUnsigned(), constant.IsUnsigned());
    EXPECT_EQ(actual, constant);
  }
}

TEST(
    BytecodeTest,
    Compile_WithArithmeticAndPredicate_GeneratesTypedBytecodeAndEvaluatesBatch) {
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
    GTEST_FAIL() << "failed";
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
  Expression constants =
      BinaryExpressionExp(ConstantValueExp(Value(2)), BinaryOperation::kAdd,
                          BinaryExpressionExp(ConstantValueExp(Value(3)),
                                              BinaryOperation::kMultiply,
                                              ConstantValueExp(Value(4))));

  auto program = BytecodeCompiler::Compile(constants, schema);
  if (!program.has_value()) {
    GTEST_FAIL() << "failed";
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
  Expression predicate =
      BinaryExpressionExp(ColumnValueExp("date"), BinaryOperation::kLessThan,
                          ConstantValueExp(Value::Date("1996-01-01")));

  auto program = BytecodeCompiler::Compile(predicate, schema);
  if (!program.has_value()) {
    GTEST_FAIL() << "failed";
    return;
  }
  const BytecodeProgram& date_program = *program;
  const ColumnVector output = date_program.EvaluateBatch(input);

  ASSERT_EQ(date_program.Instructions().size(), 3U);
  EXPECT_EQ(date_program.Instructions().back().opcode, BytecodeOp::kBinaryDate);
  EXPECT_EQ(output.ValueAt(0), Value(true));
}

TEST(BytecodeTest,
     EvaluateBatch_WithInt64Comparisons_MatchesAstAndJitSemantics) {
  constexpr int64_t kConstant = 17;
  const Schema schema("input", {Column("x", ValueType::kInt64)});
  DataChunk chunk(schema);
  const std::vector<std::optional<int64_t>> samples{
      -2048, 0, 16, 17, 18, 1024, std::nullopt};
  for (const std::optional<int64_t>& sample : samples) {
    chunk.Append(Row({sample ? Value(*sample) : Value()}));
  }

  const std::vector<BinaryOperation> operations{
      BinaryOperation::kEquals,      BinaryOperation::kNotEquals,
      BinaryOperation::kLessThan,    BinaryOperation::kLessThanEquals,
      BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals,
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
  Expression lhs =
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kNotEquals,
                          ConstantValueExp(Value(0)));
  Expression rhs =
      BinaryExpressionExp(ColumnValueExp("j"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1)));
  Expression conj = BinaryExpressionExp(lhs, BinaryOperation::kAnd, rhs);
  auto program = BytecodeCompiler::Compile(conj, schema);
  if (!program.has_value()) {
    FAIL();
    return;
  }
  EXPECT_TRUE(std::ranges::any_of(
      program->Instructions(), [](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kJumpIfFalse;
      }));

  Expression disj = BinaryExpressionExp(lhs, BinaryOperation::kOr, rhs);
  auto or_program = BytecodeCompiler::Compile(disj, schema);
  if (!or_program.has_value()) {
    FAIL();
    return;
  }
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
          static_cast<long long>(&instruction -
                                 or_program->Instructions().data()) +
          instruction.jump_target;
      EXPECT_GE(target, 0);
      EXPECT_LE(target,
                static_cast<long long>(or_program->Instructions().size()));
    }
  }
}

TEST(BytecodeTest, D7_RightHandSideErrorsAreSuppressedByShortCircuit) {
  // `i <> 0 AND 10 / j > 1`: rows where i = 0 must NOT evaluate 10 / j, so
  // j = 0 divides by zero ONLY on rows whose left side is TRUE or NULL
  // (identical to the AST semantics).
  const Schema schema("input", {Column("i", ValueType::kInt64),
                                Column("j", ValueType::kInt64)});
  Expression guard =
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kNotEquals,
                          ConstantValueExp(Value(0)));
  Expression division =
      BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kDivide,
                          ColumnValueExp("j"));
  Expression conj = BinaryExpressionExp(
      guard, BinaryOperation::kAnd,
      BinaryExpressionExp(division, BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(1))));
  auto program = BytecodeCompiler::Compile(conj, schema);
  if (!program.has_value()) {
    FAIL();
    return;
  }

  {
    DataChunk safe_rows(schema);
    safe_rows.Append(Row({Value(0), Value(0)}));  // FALSE AND (10/0) -> FALSE
    const ColumnVector output = program->EvaluateBatch(safe_rows);
    EXPECT_EQ(output.ValueAt(0), Value(false));
  }
  {
    DataChunk throwing_rows(schema);
    throwing_rows.Append(
        Row({Value(1), Value(0)}));  // TRUE AND (10/0) -> error
    EXPECT_THROW(static_cast<void>(program->EvaluateBatch(throwing_rows)),
                 std::exception);
  }
}

// sql_session/expression fuzz regressions (campaign R18, bugs 45-47):
// folds that erased the INT64<->DOUBLE promotion boundary let the
// bytecode VM raise integer overflow where the AST reference evaluated
// in IEEE double (or vice versa).
namespace {
void ExpectAstMatchesBytecode(const Expression& expression, const Row& row,
                              const Schema& schema) {
  std::optional<std::string> ast_error;
  std::optional<Value> ast_value;
  try {
    ast_value = expression->Evaluate(row, schema);
  } catch (const std::exception& error) {
    ast_error = error.what();
  }
  const std::optional<BytecodeProgram> program =
      BytecodeCompiler::Compile(expression, schema);
  if (!program.has_value()) {
    return;  // unsupported shape falls back to the AST evaluator
  }
  DataChunk chunk(schema);
  chunk.Append(row);
  if (ast_error.has_value()) {
    EXPECT_THROW(static_cast<void>(program->EvaluateBatch(chunk)),
                 std::exception);
    return;
  }
  const ColumnVector out = program->EvaluateBatch(chunk);
  EXPECT_EQ(out.ValueAt(0), *ast_value) << expression->ToString();
}
}  // namespace

TEST(BytecodeTest, MixedTypePromotion_FoldsMustNotEraseDoubleDomain) {
  const Schema schema("fuzz_schema", {Column("col_i", ValueType::kInt64),
                                      Column("col_d", ValueType::kDouble),
                                      Column("col_s", ValueType::kVarChar),
                                      Column("col_n", ValueType::kInt64)});
  const Expression one_d = ConstantValueExp(Value(1.0));
  const Expression one_i = ConstantValueExp(Value(int64_t{1}));
  const Expression null_c = ConstantValueExp(Value());
  const Expression ci = ColumnValueExp("col_i");
  // CASE takes the DOUBLE branch at runtime; the arithmetic chain is IEEE,
  // never a checked INT64 kernel (was: identity folds dropped the 1.0).
  Expression case_expr = CaseExpressionExp(
      {{ci, one_d}, {one_i, one_i}, {one_i, null_c}}, nullptr);
  Expression chain = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(case_expr, BinaryOperation::kMultiply, ci),
          BinaryOperation::kAdd, ci),
      BinaryOperation::kAdd, ci);
  ExpectAstMatchesBytecode(chain,
                           Row({Value(std::numeric_limits<int64_t>::min()),
                                Value(0.0), Value(std::string("a")), Value()}),
                           schema);
}

TEST(BytecodeTest, UniformCaseResult_FoldKeepsBranchValueType) {
  const Schema schema("fuzz_schema", {Column("col_i", ValueType::kInt64),
                                      Column("col_d", ValueType::kDouble),
                                      Column("col_s", ValueType::kVarChar),
                                      Column("col_n", ValueType::kInt64)});
  const Expression ci = ColumnValueExp("col_i");
  // THEN 1.0 / ELSE 1 agree textually but not in type: the fold must not
  // substitute the ELSE's INT64 for the taken DOUBLE branch.
  Expression case_expr = CaseExpressionExp({{ci, ConstantValueExp(Value(1.0))}},
                                           ConstantValueExp(Value(int64_t{1})));
  Expression sum = BinaryExpressionExp(
      BinaryExpressionExp(
          BinaryExpressionExp(case_expr, BinaryOperation::kAdd,
                              UnaryExpressionExp(ci, UnaryOperation::kIsTrue)),
          BinaryOperation::kAdd, ci),
      BinaryOperation::kAdd, ci);
  ExpectAstMatchesBytecode(
      sum,
      Row({Value(std::numeric_limits<int64_t>::max()),
           Value(std::numeric_limits<double>::quiet_NaN()), Value(), Value()}),
      schema);
}

TEST(BytecodeTest, Int64PlusBool_UsesAstDoublePromotionNotIntKernel) {
  const Schema schema("fuzz_schema", {Column("col_i", ValueType::kInt64),
                                      Column("col_d", ValueType::kDouble),
                                      Column("col_s", ValueType::kVarChar),
                                      Column("col_n", ValueType::kInt64)});
  const Expression ci = ColumnValueExp("col_i");
  Expression sum = BinaryExpressionExp(
      ConstantValueExp(Value(int64_t{1})), BinaryOperation::kAdd,
      UnaryExpressionExp(ci, UnaryOperation::kIsTrue));
  ExpectAstMatchesBytecode(sum,
                           Row({Value(std::numeric_limits<int64_t>::max()),
                                Value(0.0), Value(), Value()}),
                           schema);
}

TEST(BytecodeTest, StringSetRespectsRuntimeCollation) {
  const Schema schema("input", {Column("s", ValueType::kVarChar)});
  const auto expression = InExpressionExp(
      ColumnValueExp("s"),
      {ConstantValueExp(Value("ABC")), ConstantValueExp(Value())});
  const auto program = BytecodeCompiler::Compile(expression, schema);
  ASSERT_TRUE(program.has_value());
  ASSERT_TRUE(std::ranges::any_of(program->Instructions(), [](const auto& op) {
    return op.opcode == BytecodeOp::kInStringSet;
  }));
  for (const char* text : {"abc", "missing"}) {
    const Row row({Value(text).WithCollation(2)});
    const auto expected = expression->TryEvaluate(row, schema);
    const auto actual = program->TryEvaluateRow(row);
    ASSERT_TRUE(expected.HasValue());
    ASSERT_TRUE(actual.HasValue());
    EXPECT_EQ(actual.Value(), expected.Value());
  }
}

TEST(BytecodeTest, InList_EvaluatesChildOnceAndMatchesAst) {
  // `x IN (a, b, ...)` must evaluate the child once like TryMembership:
  // the lowering keeps the child on the stack (kDup) instead of emitting an
  // OR-of-equalities chain that re-evaluates it per candidate.
  const Schema schema("input", {Column("i", ValueType::kInt64),
                                Column("j", ValueType::kInt64)});
  Expression membership = InExpressionExp(
      BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(int64_t{1}))),
      {ColumnValueExp("j"), ConstantValueExp(Value(int64_t{7})),
       ConstantValueExp(Value(int64_t{9}))});
  const auto program = BytecodeCompiler::Compile(membership, schema);
  ASSERT_TRUE(program.has_value());
  const auto child_loads = static_cast<long>(std::ranges::count_if(
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
      // assumes success
      program->Instructions(),
      [&](const BytecodeInstruction&
              instruction) {  // NOLINT(bugprone-unchecked-optional-access)
        return instruction.opcode == BytecodeOp::kLoadColumn &&
               instruction.operand == schema.Offset(ColumnName("i"));
      }));
  EXPECT_EQ(child_loads, 1);
  // Counting only kLoadColumn(i) would stay green if the rest of the child
  // subtree (`+ 1`) were re-emitted once per candidate. Count the whole
  // three-instruction child pattern: its column load, its constant, and its
  // add must each appear exactly once in the whole instruction stream.
  const auto& constants = program->Constants();
  const auto child_adds = static_cast<long>(std::ranges::count_if(
      program->Instructions(), [](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kBinaryInt64 &&
               instruction.binary == BinaryOperation::kAdd;
      }));
  EXPECT_EQ(child_adds, 1);
  const auto child_constants = static_cast<long>(std::ranges::count_if(
      program->Instructions(), [&](const BytecodeInstruction& instruction) {
        return instruction.opcode == BytecodeOp::kLoadConstant &&
               instruction.operand < constants.size() &&
               constants[instruction.operand] == Value(int64_t{1});
      }));
  EXPECT_EQ(child_constants, 1);
  EXPECT_TRUE(std::ranges::any_of(
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
      // assumes success
      program->Instructions(),
      [](const BytecodeInstruction&
             instruction) {  // NOLINT(bugprone-unchecked-optional-access)
        return instruction.opcode == BytecodeOp::kDup;
      }));

  const std::vector<Row> rows{
      Row({Value(2), Value(3)}),  // i+1=3 hits j=3 -> TRUE
      Row({Value(2), Value(0)}),  // i+1=3 misses -> FALSE
      Row({Value(6), Value(0)}),  // i+1=7 hits constant -> TRUE
      Row({Value(8), Value(0)}),  // i+1=9 hits constant -> TRUE
      Row({Value(), Value(3)}),   // NULL child -> NULL
      Row({Value(5), Value()}),   // 6 misses, NULL candidate -> NULL
      Row({Value(4), Value()}),   // 5 misses j=NULL, misses 7 -> NULL
  };
  for (const Row& row : rows) {
    ExpectAstMatchesBytecode(membership, row, schema);
  }
}

TEST(BytecodeTest, InList_ShortCircuitsLaterCandidates) {
  // `i IN (1, 10 / j)`: a match on the first candidate must not evaluate the
  // division at all (the AST's early return), and a miss must surface its
  // error -- identical error-occurrence conditions on both paths.
  const Schema schema("input", {Column("i", ValueType::kInt64),
                                Column("j", ValueType::kInt64)});
  Expression membership = InExpressionExp(
      ColumnValueExp("i"),
      {ConstantValueExp(Value(int64_t{1})),
       BinaryExpressionExp(ConstantValueExp(Value(10)),
                           BinaryOperation::kDivide, ColumnValueExp("j"))});
  const auto program = BytecodeCompiler::Compile(membership, schema);
  ASSERT_TRUE(program.has_value());

  {
    DataChunk hit(schema);
    hit.Append(Row({Value(1), Value(0)}));  // 1 matches; 10/0 never runs
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
    // assumes success
    const ColumnVector output = program->EvaluateBatch(
        hit);  // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_EQ(output.ValueAt(0), Value(true));
  }
  {
    DataChunk miss(schema);
    miss.Append(Row({Value(2), Value(0)}));  // no match -> 10/0 raises
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer
    // assumes success
    EXPECT_THROW(static_cast<void>(program->EvaluateBatch(
                     miss)),  // NOLINT(bugprone-unchecked-optional-access)
                 std::exception);
  }
  ExpectAstMatchesBytecode(membership, Row({Value(5), Value(2)}), schema);
  ExpectAstMatchesBytecode(membership, Row({Value(4), Value(2)}), schema);
  ExpectAstMatchesBytecode(membership, Row({Value(), Value(0)}), schema);
}

TEST(BytecodeTest, ExtractDatePart_LowersToDedicatedOpcodeAndMatchesAst) {
  const Schema schema(
      "dates", {Column("d", ValueType::kDate), Column("s", ValueType::kVarChar),
                Column("n", ValueType::kInt64)});
  DataChunk input(schema);
  input.Append(Row({Value::Date("1996-03-13"), Value("1997-12-01"), Value(5)}));
  input.Append(Row({Value(), Value("2000-01-31"), Value()}));
  // A malformed VARCHAR slice raises exactly like the AST evaluator; keep it
  // out of the batch chunk, whose wrapper turns errors into exceptions.
  DataChunk clean(schema);
  clean.Append(Row({Value::Date("1996-03-13"), Value("1997-12-01"), Value(5)}));
  clean.Append(Row({Value(), Value("2000-01-31"), Value()}));
  Row malformed({Value::Date("1996-03-13"), Value("short"), Value(7)});

  const std::vector<std::pair<const char*, int64_t>> parts{
      {"extract_year", 1996}, {"extract_month", 3}, {"extract_day", 13}};
  for (const auto& [name, expected] : parts) {
    const Expression expr = FunctionCallExp(name, {ColumnValueExp("d")});
    auto program = BytecodeCompiler::Compile(expr, schema);
    ASSERT_TRUE(program.has_value()) << name;
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(program->Instructions().back().opcode,
              BytecodeOp::kExtractDatePart)
        << name;
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    const ColumnVector output = program->EvaluateBatch(clean);
    EXPECT_EQ(output.ValueAt(0), Value(expected)) << name;
    EXPECT_TRUE(output.ValueAt(1).IsNull()) << name;
    // The row-based VM must agree with the batch VM and the AST.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    auto row_result = program->TryEvaluateRow(input.RowAt(0));
    ASSERT_TRUE(row_result.HasValue()) << name;
    EXPECT_EQ(row_result.Value(), Value(expected)) << name;
  }
  // VARCHAR input slices the same fixed-width fields.
  const Expression varchar_year =
      FunctionCallExp("extract_year", {ColumnValueExp("s")});
  auto varchar_program = BytecodeCompiler::Compile(varchar_year, schema);
  ASSERT_TRUE(varchar_program.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  const ColumnVector varchar_output = varchar_program->EvaluateBatch(clean);
  EXPECT_EQ(varchar_output.ValueAt(0), Value(1997));
  EXPECT_EQ(varchar_output.ValueAt(1), Value(2000));
  // A malformed VARCHAR slice raises exactly like the AST evaluator.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_FALSE(varchar_program->TryEvaluateRow(malformed).HasValue());
  // Non-date/non-string input raises on both paths.
  const Expression int_year =
      FunctionCallExp("extract_year", {ColumnValueExp("n")});
  auto int_program = BytecodeCompiler::Compile(int_year, schema);
  ASSERT_TRUE(int_program.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  auto int_result = int_program->TryEvaluateRow(input.RowAt(0));
  EXPECT_FALSE(int_result.HasValue());
  auto ast_result = int_year->TryEvaluate(input.RowAt(0), schema);
  EXPECT_FALSE(ast_result.HasValue());
}

TEST(BytecodeTest, SearchedCase_LowersToConditionalJumpsAndMatchesAst) {
  const Schema schema(
      "cases", {Column("i", ValueType::kInt64), Column("d", ValueType::kDouble),
                Column("s", ValueType::kVarChar)});
  // Rows cover every condition verdict: TRUE, FALSE, NULL, and no-match
  // (NULL ELSE result).  Conditions mix column compares and IS NULL so
  // both the TRUE and the fall-through legs get exercised, including the
  // multi-WHEN fall-through that must pop every skipped condition.
  struct RowCase {
    Row row;
    std::string label;
  };
  const std::vector<RowCase> rows{
      {Row({Value(int64_t{5}), Value(1.5), Value(std::string("x"))}),
       "all-true"},
      {Row({Value(), Value(), Value()}), "all-null"},
  };
  // when_clauses: i > 1 -> d * 2; s IS NOT NULL -> 0.5; ELSE 99.0.  Branch
  // types must unify (ResultType contract), so every branch is DOUBLE.
  const Expression case_expr = CaseExpressionExp(
      {{BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                            ConstantValueExp(Value(int64_t{1}))),
        BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kMultiply,
                            ConstantValueExp(Value(2.0)))},
       {UnaryExpressionExp(ColumnValueExp("s"), UnaryOperation::kIsNotNull),
        ConstantValueExp(Value(0.5))}},
      ConstantValueExp(Value(99.0)));
  for (const RowCase& row_case : rows) {
    ExpectAstMatchesBytecode(case_expr, row_case.row, schema);
  }
  // No ELSE: the no-match path yields NULL exactly like TryPickBranch.
  const Expression case_no_else = CaseExpressionExp(
      {{BinaryExpressionExp(ColumnValueExp("i"), BinaryOperation::kGreaterThan,
                            ConstantValueExp(Value(int64_t{100}))),
        ColumnValueExp("d")}},
      nullptr);
  ExpectAstMatchesBytecode(case_no_else,
                           Row({Value(int64_t{5}), Value(), Value()}), schema);
  ExpectAstMatchesBytecode(
      case_no_else, Row({Value(int64_t{200}), Value(2.5), Value()}), schema);
  ExpectAstMatchesBytecode(case_no_else, Row({Value(), Value(), Value()}),
                           schema);
  // A program containing the CASE lowering now runs on the fast
  // interpreter (conditional jumps are part of its opcode set) and every
  // shape above still matches the AST row for row.
  auto program = BytecodeCompiler::Compile(case_expr, schema);
  ASSERT_TRUE(program.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_TRUE(program->FastInterpretable());
}

// T1950: TryEvaluateRowFast bounds-checked the kLoadColumn operand on every
// use; the canonical interpreter indexed Row unchecked, so applying a
// program compiled for a wider schema to a narrower row (residual filters,
// projections) read an out-of-range Value.
TEST(BytecodeTest, CanonicalRowEvaluatorRejectsOutOfRangeLoadColumn) {
  Column column("a", ValueType::kInt64);
  const Schema schema("input", {column, Column("b", ValueType::kInt64)});
  const auto program = BytecodeCompiler::Compile(ColumnValueExp("b"), schema);
  ASSERT_TRUE(program.has_value());
  if (!program) {
    return;
  }

  const Row narrow{std::vector<Value>{Value(5)}};
  const auto rejected = program->TryEvaluateRow(narrow);
  ASSERT_FALSE(rejected.HasValue());
  EXPECT_EQ(rejected.GetStatus().GetCode(), StatusCode::kInvalidArgument);

  const Row wide{std::vector<Value>{Value(5), Value(7)}};
  const auto ok = program->TryEvaluateRow(wide);
  ASSERT_TRUE(ok.HasValue());
  EXPECT_EQ(ok.Value(), Value(7));
}

// The INT64 3-instruction columnar fast loop must agree with the canonical
// row interpreter for results, selection vectors and overflow errors.
TEST(BytecodeTest, ColumnarInt64BinaryLoopMatchesRowInterpreter) {
  const Schema schema("input", {Column("a", ValueType::kInt64),
                                Column("b", ValueType::kInt64)});
  const std::vector<int64_t> a{1, -5, 7, 0, 123456789, -42, 9, 3};
  const std::vector<int64_t> b{2, 5, -7, 3, -987654321, -42, 0, 3};
  const std::vector<uint32_t> sel{6, 0, 3, 7, 2};
  const BytecodeColumnView av{.type = ValueType::kInt64, .ints = a.data()};
  const BytecodeColumnView bv{.type = ValueType::kInt64, .ints = b.data()};
  const std::vector<BytecodeColumnView> cols{av, bv};
  const std::vector<BinaryOperation> ops{BinaryOperation::kAdd,
                                         BinaryOperation::kSubtract,
                                         BinaryOperation::kMultiply,
                                         BinaryOperation::kEquals,
                                         BinaryOperation::kNotEquals,
                                         BinaryOperation::kLessThan,
                                         BinaryOperation::kLessThanEquals,
                                         BinaryOperation::kGreaterThan,
                                         BinaryOperation::kGreaterThanEquals};
  for (const BinaryOperation op : ops) {
    const std::vector<Expression> shapes{
        BinaryExpressionExp(ColumnValueExp("a"), op, ColumnValueExp("b")),
        BinaryExpressionExp(ColumnValueExp("a"), op,
                            ConstantValueExp(Value(int64_t{3}))),
        BinaryExpressionExp(ConstantValueExp(Value(int64_t{3})), op,
                            ColumnValueExp("b"))};
    for (const Expression& expression : shapes) {
      const auto program = BytecodeCompiler::Compile(expression, schema);
      if (!program.has_value()) {
        ADD_FAILURE() << "bytecode compilation failed";
        return;
      }
      ASSERT_EQ(program->Instructions().size(), 3U);
      for (const bool use_sel : {false, true}) {
        const size_t n = use_sel ? sel.size() : a.size();
        std::vector<int64_t> ints;
        BytecodeColumnSink sink;
        sink.ints = &ints;
        Status error;
        ASSERT_EQ(program->TryEvaluateColumnarBatch(
                      cols, use_sel ? sel.data() : nullptr, n, &sink, &error),
                  BytecodeProgram::FastEval::kDone);
        ASSERT_EQ(ints.size(), n);
        EXPECT_EQ(sink.type, ValueType::kInt64);
        for (size_t i = 0; i < n; ++i) {
          const size_t r = use_sel ? sel[i] : i;
          const auto expected =
              program->TryEvaluateRow(Row({Value(a[r]), Value(b[r])}));
          ASSERT_TRUE(expected.HasValue());
          EXPECT_EQ(Value(ints[i]), expected.Value()) << "row " << r;
        }
      }
    }
  }
  // Overflow is reported, and the sink is restored.
  const std::vector<int64_t> big{std::numeric_limits<int64_t>::max(), 1};
  const BytecodeColumnView bigv{.type = ValueType::kInt64, .ints = big.data()};
  const std::vector<BytecodeColumnView> big_cols{bigv, bigv};
  const auto add = BytecodeCompiler::Compile(
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd,
                          ColumnValueExp("b")),
      schema);
  if (!add.has_value()) {
    ADD_FAILURE() << "bytecode compilation failed";
    return;
  }
  std::vector<int64_t> ints;
  BytecodeColumnSink sink;
  sink.ints = &ints;
  Status error;
  EXPECT_EQ(add->TryEvaluateColumnarBatch(big_cols, nullptr, 2, &sink, &error),
            BytecodeProgram::FastEval::kError);
  EXPECT_TRUE(ints.empty());

  // Division writes DOUBLE results, while modulo writes INT64 results.
  // Both columnar paths must agree with the row interpreter.
  const std::vector<int64_t> da{100, -100, 7, -7, 123456789};
  const std::vector<int64_t> db{3, 3, -2, -2, 42};
  const BytecodeColumnView dav{.type = ValueType::kInt64, .ints = da.data()};
  const BytecodeColumnView dbv{.type = ValueType::kInt64, .ints = db.data()};
  const std::vector<BytecodeColumnView> div_cols{dav, dbv};
  for (const BinaryOperation op :
       {BinaryOperation::kDivide, BinaryOperation::kModulo}) {
    const std::vector<Expression> shapes{
        BinaryExpressionExp(ColumnValueExp("a"), op, ColumnValueExp("b")),
        BinaryExpressionExp(ColumnValueExp("a"), op,
                            ConstantValueExp(Value(int64_t{3}))),
        BinaryExpressionExp(ConstantValueExp(Value(int64_t{100})), op,
                            ColumnValueExp("b"))};
    for (const Expression& expression : shapes) {
      const auto program = BytecodeCompiler::Compile(expression, schema);
      ASSERT_TRUE(program.has_value());
      std::vector<int64_t> div_ints;
      std::vector<double> div_doubles;
      BytecodeColumnSink div_sink;
      div_sink.ints = &div_ints;
      div_sink.doubles = &div_doubles;
      Status div_error;
      ASSERT_EQ(program->TryEvaluateColumnarBatch(div_cols, nullptr, da.size(),
                                                  &div_sink, &div_error),
                BytecodeProgram::FastEval::kDone);
      const bool floating = op == BinaryOperation::kDivide;
      ASSERT_EQ(div_sink.type,
                floating ? ValueType::kDouble : ValueType::kInt64);
      ASSERT_EQ(floating ? div_doubles.size() : div_ints.size(), da.size());
      for (size_t i = 0; i < da.size(); ++i) {
        const auto expected =
            program->TryEvaluateRow(Row({Value(da[i]), Value(db[i])}));
        ASSERT_TRUE(expected.HasValue());
        const Value actual =
            floating ? Value(div_doubles[i]) : Value(div_ints[i]);
        EXPECT_EQ(actual, expected.Value()) << "row " << i;
      }
    }
  }

  const auto expect_columnar_error = [&](BinaryOperation op, int64_t left,
                                         int64_t right) {
    const auto expression =
        BinaryExpressionExp(ColumnValueExp("a"), op, ColumnValueExp("b"));
    const auto program = BytecodeCompiler::Compile(expression, schema);
    EXPECT_TRUE(program.has_value());
    if (!program) {
      return;
    }
    // Both paths must reject the same row.
    EXPECT_FALSE(
        program->TryEvaluateRow(Row({Value(left), Value(right)})).HasValue());
    const int64_t lefts[] = {left, 5};
    const int64_t rights[] = {right, 3};
    const BytecodeColumnView left_view{.type = ValueType::kInt64,
                                       .ints = lefts};
    const BytecodeColumnView right_view{.type = ValueType::kInt64,
                                        .ints = rights};
    const std::vector<BytecodeColumnView> error_cols{left_view, right_view};
    std::vector<int64_t> error_ints{11, 22};
    std::vector<double> error_doubles{11.0, 22.0};
    BytecodeColumnSink error_sink;
    error_sink.ints = &error_ints;
    error_sink.doubles = &error_doubles;
    Status error_status;
    EXPECT_EQ(program->TryEvaluateColumnarBatch(error_cols, nullptr, 2,
                                                &error_sink, &error_status),
              BytecodeProgram::FastEval::kError);
    // The batch aborts and the sink is restored to its pre-batch state.
    EXPECT_EQ(error_ints, (std::vector<int64_t>{11, 22}));
    EXPECT_EQ(error_doubles, (std::vector<double>{11.0, 22.0}));
  };
  expect_columnar_error(BinaryOperation::kDivide, 7, 0);
  expect_columnar_error(BinaryOperation::kModulo,
                        std::numeric_limits<int64_t>::min(), -1);
}

// P1952: when a columnar batch demotes its uniform sink to the fallback
// column, every fallback entry must own its VARCHAR payload: the row- and
// chunk-level consumers hand the Value onward after the source column views
// die (ColumnarColumn::EnsureFallbackValueOwned materializes owned_data).
// The demote trigger row and every post-demote row are pushed as
// FastValueFromSlot's borrowed view; Value's copy/move slow paths
// (InitCopySlow/InitMoveSlow) must materialize it. Guards the contract the
// earlier fallback rows relied on explicit CopyFromStringView for.
TEST(BytecodeTest, DemotedFallbackStringsOwnTheirPayload) {
  const Schema schema("input", {Column("v", ValueType::kInt64)});
  const auto program = BytecodeCompiler::Compile(ColumnValueExp("v"), schema);
  ASSERT_TRUE(program.has_value());
  if (!program) {
    return;
  }

  // Non-uniform input column: row 0 pins the sink to INT64, row 1 triggers
  // the demote with a VARCHAR value, row 2 exercises the post-demote arm.
  const std::vector<Value> input{Value(int64_t{7}), Value("first"),
                                 Value("second")};
  const BytecodeColumnView view{.is_uniform = false, .fallback = input.data()};
  const std::vector<BytecodeColumnView> cols{view};

  std::vector<int64_t> ints;
  std::vector<std::string_view> strings;
  std::vector<Value> fallback;
  BytecodeColumnSink sink;
  sink.ints = &ints;
  sink.strings = &strings;
  sink.fallback = &fallback;
  Status error;
  ASSERT_EQ(program->TryEvaluateColumnarBatch(cols, nullptr, input.size(),
                                              &sink, &error),
            BytecodeProgram::FastEval::kDone);
  ASSERT_EQ(fallback.size(), 3U);
  EXPECT_EQ(fallback[0], Value(int64_t{7}));
  ASSERT_EQ(fallback[1].type, ValueType::kVarChar);
  EXPECT_EQ(fallback[1].value.varchar_value, "first");
  EXPECT_FALSE(fallback[1].owned_data.empty());
  ASSERT_EQ(fallback[2].type, ValueType::kVarChar);
  EXPECT_EQ(fallback[2].value.varchar_value, "second");
  EXPECT_FALSE(fallback[2].owned_data.empty());
}

}  // namespace tinylamb
