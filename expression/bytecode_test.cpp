/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/bytecode.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
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

}  // namespace tinylamb
