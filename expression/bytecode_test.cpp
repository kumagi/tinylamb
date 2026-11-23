/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "common/test_util.hpp"
#include "executor/data_chunk.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/parameter_slot.hpp"
#include "gtest/gtest.h"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(BytecodeTest, IsTrueAndFalseRejectNonBooleanIntegerValues) {
  const Schema schema("input", {Column("x", ValueType::kInt64)});
  for (const auto op :
       {UnaryOperation::kIsTrue, UnaryOperation::kIsNotTrue,
        UnaryOperation::kIsFalse, UnaryOperation::kIsNotFalse}) {
    const auto expression = UnaryExpressionExp(ColumnValueExp("x"), op);
    const auto program = BytecodeCompiler::Compile(expression, schema);
    ASSERT_TRUE(program.has_value());
    for (const Value& value :
         {Value(), Value(0), Value(1), Value(-1), Value(2)}) {
      const Row row({value});
      DataChunk input(schema);
      input.Append(row);
      const auto expected = expression->TryEvaluate(row, schema);
      const auto actual = program->TryEvaluateBatch(input);
      EXPECT_EQ(actual.GetStatus(), expected.GetStatus());
      if (expected.HasValue() && actual.HasValue()) {
        EXPECT_EQ(actual.Value().ValueAt(0), expected.Value());
      }
    }
  }
}

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

TEST(BytecodeTest, OversizedSchemaNameFallsBackToUncachedCompilation) {
  constexpr size_t limit = std::numeric_limits<bin_size_t>::max();
  const Schema schema(std::string(limit + 1, 'a'),
                      {Column("k", ValueType::kInt64)});
  const auto expression = ColumnValueExp("k");
  EXPECT_FALSE(ScalarExpressionCacheKey({expression}, schema));
  const auto program = BytecodeCompiler::CompileRowCached(expression, schema);
  ASSERT_TRUE(program.has_value());
  ASSERT_SUCCESS_AND_EQ(TryEvaluateRowAuto(*program, Row({Value(7)})),
                        Value(7));
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
    const Row row({Value(text).WithCollation(
        static_cast<uint8_t>(Collation::kCaseInsensitive))});
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

TEST(BytecodeTest, SimdInRangeMatchesAstAndJitAcrossExtremes) {
  ScopedEnvVar guard("TINYLAMB_DISABLE_SIMD_IN_RANGE", nullptr);

  const Schema schema("t", {Column("x", ValueType::kInt64)});
  const int64_t kMin = std::numeric_limits<int64_t>::min();
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  struct Case {
    int64_t lo;
    int64_t hi;
    bool strict_lo;
    bool strict_hi;
    bool expect_fused;
  };
  const std::vector<Case> cases = {
      {-5, 5, false, false, true},
      {kMin, kMax, false, false, true},
      {kMin, kMin, false, false, true},
      {kMax, kMax, false, false, true},
      {10, 20, true, true, true},
      {20, 10, false, false, false},  // lo > hi must not fuse
  };
  const std::vector<Value> test_vals = {
      Value(),   Value(kMin),     Value(kMin + 1), Value(-6), Value(-5),
      Value(0),  Value(5),        Value(10),       Value(11), Value(19),
      Value(20), Value(kMax - 1), Value(kMax)};

  for (const Case& c : cases) {
    Expression expr = BinaryExpressionExp(
        BinaryExpressionExp(ColumnValueExp("x"),
                            c.strict_lo ? BinaryOperation::kGreaterThan
                                        : BinaryOperation::kGreaterThanEquals,
                            ConstantValueExp(Value(c.lo))),
        BinaryOperation::kAnd,
        BinaryExpressionExp(ColumnValueExp("x"),
                            c.strict_hi ? BinaryOperation::kLessThan
                                        : BinaryOperation::kLessThanEquals,
                            ConstantValueExp(Value(c.hi))));
    auto prog = BytecodeCompiler::Compile(expr, schema,
                                          BytecodeCompiler::Context::kFilter);
    ASSERT_TRUE(prog.has_value());
    bool has_range_op = false;
    for (const auto& inst : prog->Instructions()) {
      if (inst.opcode == BytecodeOp::kInt64InRange) {
        has_range_op = true;
      }
    }
    EXPECT_EQ(has_range_op, c.expect_fused);

    auto jit = JitBranchlessProgram::Compile(*prog, schema);
    for (const Value& v : test_vals) {
      Row row({v});
      auto ast_or = expr->TryEvaluate(row, schema);
      ASSERT_TRUE(ast_or.HasValue());
      if (c.lo > c.hi && !v.IsNull()) {
        EXPECT_EQ(ast_or.Value(), Value(false));
      }
      auto vm_or = prog->TryEvaluateRow(row);
      ASSERT_TRUE(vm_or.HasValue());
      Value fast_val;
      Status fast_err;
      ASSERT_EQ(prog->TryEvaluateRowFast(row, &fast_val, &fast_err),
                BytecodeProgram::FastEval::kDone);
      if (c.lo > c.hi && !v.IsNull()) {
        EXPECT_EQ(ast_or.Value(), Value(false));
      }
      EXPECT_EQ(vm_or.Value(), ast_or.Value());
      EXPECT_EQ(fast_val, ast_or.Value());
      if (jit.has_value()) {
        std::vector<int64_t> col_vals{v.IsNull() ? int64_t{0}
                                                 : v.value.int_value};
        std::vector<uint8_t> col_nulls{static_cast<uint8_t>(v.IsNull())};
        const JitColumnInput cin{.values = col_vals.data(),
                                 .nulls = col_nulls.data(),
                                 .has_nulls = v.IsNull(),
                                 .has_bounds = false,
                                 .min_value = 0,
                                 .max_value = 0};
        std::array<int64_t, 1> out_vals{};
        std::array<uint8_t, 1> out_nulls{};
        bool any_null = false;
        bool used_unchecked = false;
        ASSERT_TRUE(jit->Evaluate(std::span<const JitColumnInput>(&cin, 1), 1,
                                  out_vals.data(), out_nulls.data(), &any_null,
                                  &used_unchecked));
        EXPECT_TRUE(used_unchecked);
        if (ast_or.Value().IsNull()) {
          EXPECT_NE(out_nulls[0], 0U);
        } else {
          EXPECT_EQ(out_nulls[0], 0U);
          EXPECT_EQ(out_vals[0], ast_or.Value().value.int_value);
        }
      }
    }

    // Columnar tight loop on non-null INT64 inputs
    std::vector<int64_t> nonnull_inputs;
    for (const Value& v : test_vals) {
      if (!v.IsNull()) {
        nonnull_inputs.push_back(v.value.int_value);
      }
    }
    BytecodeColumnView col_view{.type = ValueType::kInt64,
                                .is_unsigned = false,
                                .is_uniform = true,
                                .has_null = false,
                                .ints = nonnull_inputs.data()};
    std::vector<int64_t> out_ints;
    BytecodeColumnSink sink;
    sink.type = ValueType::kInt64;
    sink.ints = &out_ints;
    Status err;
    ASSERT_EQ(prog->TryEvaluateColumnarBatch(
                  std::span<const BytecodeColumnView>(&col_view, 1), nullptr,
                  nonnull_inputs.size(), &sink, &err),
              BytecodeProgram::FastEval::kDone);
    ASSERT_EQ(out_ints.size(), nonnull_inputs.size());
    for (size_t i = 0; i < nonnull_inputs.size(); ++i) {
      auto ast_or = expr->TryEvaluate(Row({Value(nonnull_inputs[i])}), schema);
      ASSERT_TRUE(ast_or.HasValue());
      EXPECT_EQ(out_ints[i], ast_or.Value().value.int_value);
    }
  }
}

TEST(BytecodeTest, ReciprocalDivModMatchesAstAndJitAcrossExtremes) {
  ScopedEnvVar guard("TINYLAMB_DISABLE_RECIPROCAL_DIVMOD", nullptr);

  const Schema schema("t", {Column("x", ValueType::kInt64)});
  const int64_t kMin = std::numeric_limits<int64_t>::min();
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  const std::vector<int64_t> divisors = {
      kMin, -1000000007LL, -7, -2, -1, 0, 1, 2, 3, 7, 1000000007LL, kMax};
  const std::vector<Value> xs = {Value(),      Value(kMin),     Value(kMin + 1),
                                 Value(-1000), Value(-7),       Value(-1),
                                 Value(0),     Value(1),        Value(7),
                                 Value(1000),  Value(kMax - 1), Value(kMax)};

  for (const int64_t d : divisors) {
    for (const bool is_div : {true, false}) {
      Expression expr =
          FunctionCallExp(is_div ? "div" : "mod",
                          {ColumnValueExp("x"), ConstantValueExp(Value(d))});
      auto prog = BytecodeCompiler::Compile(expr, schema,
                                            BytecodeCompiler::Context::kValue);
      const bool expect_recip = (d != 0 && d != 1 && d != -1);
      EXPECT_EQ(prog.has_value(), expect_recip)
          << "d=" << d << " div=" << is_div;
      if (!prog.has_value()) {
        continue;
      }
      bool has_recip = false;
      for (const auto& inst : prog->Instructions()) {
        if ((is_div && inst.opcode == BytecodeOp::kDivInt64Const) ||
            (!is_div && inst.opcode == BytecodeOp::kModInt64Const)) {
          has_recip = true;
        }
      }
      EXPECT_EQ(has_recip, expect_recip) << "d=" << d << " div=" << is_div;

      auto jit = JitBranchlessProgram::Compile(*prog, schema);
      for (const Value& x : xs) {
        Row row({x});
        auto ast_or = expr->TryEvaluate(row, schema);
        auto vm_or = prog->TryEvaluateRow(row);
        EXPECT_EQ(vm_or.HasValue(), ast_or.HasValue());
        if (ast_or.HasValue() && vm_or.HasValue()) {
          EXPECT_EQ(vm_or.Value(), ast_or.Value());
          Value fast_val;
          Status fast_err;
          ASSERT_EQ(prog->TryEvaluateRowFast(row, &fast_val, &fast_err),
                    BytecodeProgram::FastEval::kDone);
          EXPECT_EQ(fast_val, ast_or.Value());
          if (jit.has_value()) {
            std::vector<int64_t> col_vals{x.IsNull() ? int64_t{0}
                                                     : x.value.int_value};
            std::vector<uint8_t> col_nulls{static_cast<uint8_t>(x.IsNull())};
            const JitColumnInput cin{.values = col_vals.data(),
                                     .nulls = col_nulls.data(),
                                     .has_nulls = x.IsNull(),
                                     .has_bounds = false,
                                     .min_value = 0,
                                     .max_value = 0};
            std::array<int64_t, 1> out_vals{};
            std::array<uint8_t, 1> out_nulls{};
            bool any_null = false;
            bool used_unchecked = false;
            ASSERT_TRUE(jit->Evaluate(std::span<const JitColumnInput>(&cin, 1),
                                      1, out_vals.data(), out_nulls.data(),
                                      &any_null, &used_unchecked));
            EXPECT_TRUE(used_unchecked);
            if (ast_or.Value().IsNull()) {
              EXPECT_NE(out_nulls[0], 0U);
            } else {
              EXPECT_EQ(out_nulls[0], 0U);
              EXPECT_EQ(out_vals[0], ast_or.Value().value.int_value);
            }
          }
        }
      }
    }
  }
}

TEST(BytecodeTest, FilterEvalOrderReordersErrorFreeAndPreservesThrowingOrder) {
  ScopedEnvVar guard("TINYLAMB_DISABLE_FILTER_ORDER", nullptr);

  const Schema schema(
      "t", {Column("k", ValueType::kInt64), Column("s", ValueType::kVarChar)});
  // Safe error-free predicate: `s LIKE '%foo%bar%' AND k = 5` should move
  // `k = 5` (load column 0) ahead of `s LIKE ...` (load column 1).
  Expression safe_pred = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLike,
                          ConstantValueExp(Value("%foo%bar%"))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kEquals,
                          ConstantValueExp(Value(int64_t{5}))));
  auto safe_prog = BytecodeCompiler::Compile(
      safe_pred, schema, BytecodeCompiler::Context::kFilter);
  ASSERT_TRUE(safe_prog.has_value());
  ASSERT_FALSE(safe_prog->Instructions().empty());
  EXPECT_EQ(safe_prog->Instructions()[0].opcode, BytecodeOp::kLoadColumn);
  EXPECT_EQ(safe_prog->Instructions()[0].operand, 0U);
  for (const Row& r :
       {Row({Value(), Value("xfooybarz")}), Row({Value(int64_t{5}), Value()}),
        Row({Value(int64_t{5}), Value("xfooybarz")}),
        Row({Value(int64_t{7}), Value("xfooybarz")})}) {
    auto ast = safe_pred->TryEvaluate(r, schema);
    auto vm = safe_prog->TryEvaluateRow(r);
    ASSERT_TRUE(ast.HasValue());
    ASSERT_TRUE(vm.HasValue());
    EXPECT_EQ(vm.Value(), ast.Value());
  }

  // Fallible predicate: `k > 0 AND (10 % k) > 0` must NOT swap `(10 % k)`
  // ahead of `k > 0` when k = 0, preserving short-circuit FALSE without error!
  Expression throwing_pred = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp("k"), BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(int64_t{0}))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(
          BinaryExpressionExp(ConstantValueExp(Value(int64_t{10})),
                              BinaryOperation::kModulo, ColumnValueExp("k")),
          BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t{0}))));
  auto throw_prog = BytecodeCompiler::Compile(
      throwing_pred, schema, BytecodeCompiler::Context::kFilter);
  ASSERT_TRUE(throw_prog.has_value());
  const Row zero_row({Value(int64_t{0}), Value("x")});
  auto ast_zero = throwing_pred->TryEvaluate(zero_row, schema);
  auto vm_zero = throw_prog->TryEvaluateRow(zero_row);
  ASSERT_TRUE(ast_zero.HasValue());
  ASSERT_TRUE(vm_zero.HasValue());
  EXPECT_EQ(vm_zero.Value(), ast_zero.Value());
}

// An IN predicate whose child/candidate have conflicting explicit collations
// raises kInvalidArgument at evaluation (in_expression.cpp). It is therefore
// NOT error-free even though its static ValueTypes match; the filter-order
// pass must not move it ahead of a FALSE conjunct or it would surface an
// error the AST's left-to-right AND short-circuits away.
TEST(BytecodeTest, FilterOrderKeepsCollationConflictingInPredicate) {
  const Schema schema(
      "t", {Column("k", ValueType::kInt64), Column("s", ValueType::kVarChar)});
  // Constant Values carrying different collation tags: CollationOfExpression
  // sees no COLLATE() call, but HaveConflictingCollations at evaluation time
  // still rejects the pair. The static error-free analysis must therefore
  // report false so the filter-order pass never moves this predicate ahead
  // of a short-circuiting FALSE conjunct.
  Expression conflicting_in =
      InExpressionExp(ConstantValueExp(Value("abc").WithCollation(
                          static_cast<uint8_t>(Collation::kCaseInsensitive))),
                      {ConstantValueExp(Value("xyz").WithCollation(
                          static_cast<uint8_t>(Collation::kBinary)))});
  EXPECT_FALSE(AuditIsErrorFreePredicateForOrdering(conflicting_in, schema));

  // Control: a plain VARCHAR IN is genuinely error-free.
  Expression plain_in =
      InExpressionExp(ColumnValueExp("s"), {ConstantValueExp(Value("xyz"))});
  EXPECT_TRUE(AuditIsErrorFreePredicateForOrdering(plain_in, schema));

  // The conflict is still an error at evaluation time.
  const Row row({Value(int64_t{7}), Value("abc")});
  auto ast = conflicting_in->TryEvaluate(row, schema);
  ASSERT_FALSE(ast.HasValue());
  EXPECT_EQ(ast.GetStatus().GetCode(), StatusCode::kInvalidArgument);
}

// The fast row interpreter guards every stack-consuming opcode: a program
// whose stack runs dry must surface kCorrupt, never read an empty vector.
// Compiler output is always balanced, but the flag is also settable by hand
// (SetFastInterpretable), so the contract has to hold for any marked program.
TEST(BytecodeTest, FastRowEvaluatorRejectsStackUnderflow) {
  const Row row;
  Value actual;
  Status error;

  {
    BytecodeProgram program;
    program.AddInstruction(
        {.opcode = BytecodeOp::kBinaryInt64, .binary = BinaryOperation::kAdd});
    program.SetFastInterpretable(true);
    EXPECT_EQ(program.TryEvaluateRowFast(row, &actual, &error),
              BytecodeProgram::FastEval::kError);
    EXPECT_EQ(error.GetCode(), StatusCode::kCorrupt);
  }
  {
    // One operand is still short for a binary op: right pops, left underflows.
    BytecodeProgram program;
    uint16_t constant = 0;
    ASSERT_TRUE(program.AddConstant(Value(int64_t{1}), &constant));
    program.AddInstruction(
        {.opcode = BytecodeOp::kLoadConstant, .operand = constant});
    program.AddInstruction(
        {.opcode = BytecodeOp::kBinaryInt64, .binary = BinaryOperation::kAdd});
    program.SetFastInterpretable(true);
    EXPECT_EQ(program.TryEvaluateRowFast(row, &actual, &error),
              BytecodeProgram::FastEval::kError);
    EXPECT_EQ(error.GetCode(), StatusCode::kCorrupt);
  }
  {
    BytecodeProgram program;
    program.AddInstruction(
        {.opcode = BytecodeOp::kUnaryInt64, .unary = UnaryOperation::kNot});
    program.SetFastInterpretable(true);
    EXPECT_EQ(program.TryEvaluateRowFast(row, &actual, &error),
              BytecodeProgram::FastEval::kError);
    EXPECT_EQ(error.GetCode(), StatusCode::kCorrupt);
  }
  {
    BytecodeProgram program;
    program.AddInstruction({.opcode = BytecodeOp::kSubstr, .operand = 2});
    program.SetFastInterpretable(true);
    EXPECT_EQ(program.TryEvaluateRowFast(row, &actual, &error),
              BytecodeProgram::FastEval::kError);
    EXPECT_EQ(error.GetCode(), StatusCode::kCorrupt);
  }
}

// AST Evaluate is the reference: SUBSTR accepts UINT64 start/length (the
// unsigned tag shares kInt64). The start keeps its raw bit pattern (so a
// UINT64 near 2^64 is an end-relative offset) and the length saturates.
// Bytecode used to reject both with a type error at execution time only.
TEST(BytecodeTest, SubstrAcceptsUnsignedArgumentsLikeAst) {
  const Schema schema("input", {});
  const Value str = Value(std::string("abcdef"));
  const Value u64_max = Value(int64_t{-1}).WithUnsigned();
  const Value u64_max_minus_1 = Value(int64_t{-2}).WithUnsigned();
  const std::vector<Expression> expressions{
      FunctionCallExp("substr",
                      {ConstantValueExp(str), ConstantValueExp(u64_max)}),
      FunctionCallExp("substr",
                      {ConstantValueExp(str), ConstantValueExp(u64_max_minus_1),
                       ConstantValueExp(Value(int64_t{2}))}),
      FunctionCallExp(
          "substr", {ConstantValueExp(str), ConstantValueExp(Value(int64_t{2})),
                     ConstantValueExp(u64_max)}),
      FunctionCallExp("substring",
                      {ConstantValueExp(str), ConstantValueExp(u64_max)}),
  };
  for (const Expression& expression : expressions) {
    const auto program = BytecodeCompiler::Compile(expression, schema);
    ASSERT_TRUE(program.has_value()) << expression->ToString();
    if (!program) {
      continue;
    }
    const Row row;
    const auto expected = expression->TryEvaluate(row, schema);
    const auto actual = program->TryEvaluateRow(row);
    const auto automatic = TryEvaluateRowAuto(*program, row);
    ASSERT_TRUE(expected.HasValue()) << expression->ToString();
    ASSERT_TRUE(actual.HasValue())
        << expression->ToString() << ": " << actual.GetStatus().GetMessage();
    ASSERT_TRUE(automatic.HasValue())
        << expression->ToString() << ": " << automatic.GetStatus().GetMessage();
    EXPECT_EQ(actual.Value(), expected.Value()) << expression->ToString();
    EXPECT_EQ(automatic.Value(), expected.Value()) << expression->ToString();
  }
}

// Lean proof: lean/TinylambOpt/ProposedProofs/RowInMask.lean (sound)
TEST(BytecodeTest, RowInMaskMatchesAstAcrossNullsWideTuplesAndMultiWord) {
  struct ScopedEnv {
    const char* name;
    std::string prev;
    bool had_prev{false};
    ScopedEnv(const char* n, const char* val) : name(n) {
      if (const char* cur = std::getenv(name)) {
        prev = cur;
        had_prev = true;
      }
      if (val != nullptr) {
        ::setenv(name, val, 1);
      } else {
        ::unsetenv(name);
      }
    }
    ~ScopedEnv() {
      if (had_prev) {
        ::setenv(name, prev.c_str(), 1);
      } else {
        ::unsetenv(name);
      }
    }
  };

  const Schema schema(
      "t", {Column("a", ValueType::kInt64), Column("b", ValueType::kInt64)});
  const Expression struct_expr = FunctionCallExp(
      "__struct_json__",
      {ConstantValueExp(Value("f1")), ColumnValueExp("a"),
       ConstantValueExp(Value(0)), ConstantValueExp(Value("f2")),
       ColumnValueExp("b"), ConstantValueExp(Value(0))});

  std::vector<Expression> candidates;
  const int64_t kMin = std::numeric_limits<int64_t>::min();
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  candidates.push_back(ConstantValueExp(
      Value(std::string(R"({"f1":)" + std::to_string(kMin) + R"(,"f2":)" +
                        std::to_string(kMax) + "}"))));
  // Add > 64 tuples (including a duplicate and a NULL-field tuple) to span
  // multiple 64-bit bitmap words.
  for (int64_t i = 0; i < 72; ++i) {
    candidates.push_back(ConstantValueExp(
        Value(std::string(R"({"f1":)" + std::to_string(i * 3) + R"(,"f2":)" +
                          std::to_string((i * 5) + 1) + "}"))));
  }
  // Duplicate tuple in word 1 + tuple with second component NULL.
  candidates.push_back(
      ConstantValueExp(Value(std::string(R"({"f1":3,"f2":6})"))));
  candidates.push_back(
      ConstantValueExp(Value(std::string(R"({"f1":500,"f2":null})"))));

  const Expression expr = InExpressionExp(struct_expr, candidates);

  auto has_row_in_mask_op = [](const BytecodeProgram& prog) {
    return std::ranges::any_of(prog.Instructions(),
                               [](const BytecodeInstruction& inst) {
                                 return inst.opcode == BytecodeOp::kRowInMask;
                               });
  };

  std::optional<BytecodeProgram> prog_on;
  {
    ScopedEnv enable("TINYLAMB_DISABLE_ROW_IN_MASK", nullptr);
    prog_on = BytecodeCompiler::Compile(expr, schema);
  }
  ASSERT_TRUE(prog_on.has_value());
  EXPECT_TRUE(has_row_in_mask_op(*prog_on));

  std::optional<BytecodeProgram> prog_off;
  {
    ScopedEnv disable("TINYLAMB_DISABLE_ROW_IN_MASK", "1");
    prog_off = BytecodeCompiler::Compile(expr, schema);
  }
  EXPECT_FALSE(prog_off.has_value());

  const std::vector<Row> probes{
      Row({Value(kMin), Value(kMax)}),  // hit in word 0 (extreme values)
      Row({Value(3), Value(6)}),        // hit in word 0 (duplicate entry)
      Row({Value(210), Value(351)}),    // hit in word 1 (index 71)
      Row({Value(999), Value(888)}),    // clean miss (no ambiguous NULL)
      Row({Value(500),
           Value(123)}),  // partial match w/ ("f1":500,"f2":null) -> NULL
      Row({Value(500),
           Value()}),            // NULL matched vs ("f1":500,"f2":null) -> NULL
      Row({Value(), Value(6)}),  // LHS NULL matches f2=6 of (3,6) -> NULL
      Row({Value(), Value(7777)}),  // LHS NULL never matches any f2 -> FALSE
      Row({Value(), Value()}),      // both NULL -> NULL
  };

  DataChunk batch_in(schema);
  for (const Row& row : probes) {
    batch_in.Append(row);
    const auto expected = expr->TryEvaluate(row, schema);
    const auto got_on = prog_on->TryEvaluateRow(row);
    const auto got_auto = TryEvaluateRowAuto(*prog_on, row);
    ASSERT_TRUE(expected.HasValue()) << row.ToString();
    ASSERT_TRUE(got_on.HasValue()) << row.ToString();
    ASSERT_TRUE(got_auto.HasValue()) << row.ToString();
    EXPECT_EQ(got_on.Value(), expected.Value()) << row.ToString();
    EXPECT_EQ(got_auto.Value(), expected.Value()) << row.ToString();
  }

  const auto batch_out = prog_on->TryEvaluateBatch(batch_in);
  ASSERT_TRUE(batch_out.HasValue());
  ASSERT_EQ(batch_out.Value().Size(), probes.size());
  for (size_t i = 0; i < probes.size(); ++i) {
    const auto expected = expr->TryEvaluate(probes[i], schema);
    ASSERT_TRUE(expected.HasValue());
    EXPECT_EQ(batch_out.Value().ValueAt(i), expected.Value()) << "row " << i;
  }
}

}  // namespace tinylamb
